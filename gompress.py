#!/usr/bin/env python3
# gompress.py
# authored by krunch3r (https://www.github.com/krunch3r76)
# license GPL 3.0
# skeleton and utils adopted from Golem yapapi's code

MAX_PRICE_CPU_HR = "0.019"
MAX_PRICE_DUR_HR = "0.0"

from datetime import datetime, timedelta
import pathlib
import sys
from pathlib import Path, PurePosixPath
from decimal import Decimal
from lzma import LZMACompressor

from debug.mylogging import g_logger
import yapapi
from yapapi import (
    Golem,
    Task,
    WorkContext,
)
from yapapi.payload import vm
from yapapi.rest.activity import BatchTimeoutError


from utils import (
    build_parser,
    TEXT_COLOR_CYAN,
    TEXT_COLOR_DEFAULT,
    TEXT_COLOR_RED,
    TEXT_COLOR_MAGENTA,
    format_usage,
    run_golem_example,
    print_env_info,
)

from tempfile import gettempdir

from workdirectoryinfo import WorkDirectoryInfo
from ctx import CTX

try:
    moduleFilterProviderMS = False
    from gc__filterms import FilterProviderMS
except ModuleNotFoundError:
    pass
else:
    moduleFilterProviderMS = True


class MyTask(Task):
    """Task extended with reference to context"""

    def __init__(self, mainctx, data):
        self.mainctx = mainctx
        super().__init__(data)


async def main(
    ctx,
    subnet_tag,
    min_cpu_threads,
    payment_driver=None,
    payment_network=None,
    show_usage=False,
):
    package = await vm.repo(
        image_hash="5955d4f1a18eed6b90687c377156d20423466deeaa51962e8bb91292",
        # only run on provider nodes that have more than 0.5gb of RAM available
        min_mem_gib=0.5,
        # only run on provider nodes that have more than 2gb of storage space available
        min_storage_gib=2.0,
        # only run on provider nodes which a certain number of CPU threads (logical CPU cores) available
        min_cpu_threads=min_cpu_threads,
    )

    async def worker(ctx: WorkContext, tasks):
        # Set timeout for the first script executed on the provider
        script = ctx.new_script(timeout=timedelta(minutes=30))

        async for task in tasks:
            partId = task.data  # subclassed Task with id attribute
            # read range and write into temporary file
            view_to_temporary_file = task.mainctx.view_to_temporary_file(
                partId
            )  # revise to conserve memory
            # resolve to target
            if task.mainctx.precompression_level >= 0:
                path_to_remote_target = (
                    PurePosixPath("/golem/workdir") / f"part_{partId}.xz"
                )
                lzmaCompressor = LZMACompressor(
                    preset=task.mainctx.precompression_level
                )
                compressed_intermediate = lzmaCompressor.compress(
                    view_to_temporary_file.tobytes()
                )
                compressed_intermediate = (
                    compressed_intermediate + lzmaCompressor.flush()
                )  # upload_bytes does not play well with lzmaCompressor (does not flush), so ...
                script.upload_bytes(
                    compressed_intermediate,
                    path_to_remote_target,
                )
            else:
                path_to_remote_target = (
                    PurePosixPath("/golem/workdir") / f"part_{partId}"
                )
                script.upload_bytes(
                    view_to_temporary_file.tobytes(), path_to_remote_target
                )

            # run script on uploaded target
            future_result = script.run(
                "/root/xz.sh",
                path_to_remote_target.name,  # shell script is run from workdir, expects
                # filename is local to workdir
                f"-T{task.mainctx.min_threads}",
                f"-{task.mainctx.compression_level}",
            )  # output is stored by same name
            # resolve to processed target
            path_to_processed_target = PurePosixPath(f"/golem/output/part_{partId}.xz")
            local_output_file = (
                task.mainctx.work_directory_info.path_to_parts_directory
                / path_to_processed_target.name
            )
            script.download_file(path_to_processed_target, local_output_file)
            try:
                yield script
                result_dict = {}
                stdout = future_result.result().stdout
                if not stdout.startswith("OK:"):
                    task.reject_result(retry=True)
                    # this requires testing TODO
                else:
                    result_dict["checksum"] = stdout.split(":")[1][:-1]
                    result_dict["path"] = str(local_output_file.as_posix())
                    task.accept_result(result=result_dict)
            except BatchTimeoutError:
                try:
                    path_to_local_segment_file.unlink()
                except:
                    pass
                print(
                    f"{TEXT_COLOR_RED}"
                    f"Task {task} timed out on {ctx.provider_name}, time: {task.running_time}"
                    f"{TEXT_COLOR_DEFAULT}"
                )
                raise

            # reinitialize the script which we send to the engine to compress subsequent parts
            script = ctx.new_script(timeout=timedelta(minutes=30))

            if show_usage:
                raw_state = await ctx.get_raw_state()
                usage = format_usage(await ctx.get_usage())
                cost = await ctx.get_cost()
                print(
                    f"{TEXT_COLOR_MAGENTA}"
                    f" --- {ctx.provider_name} STATE: {raw_state}\n"
                    f" --- {ctx.provider_name} USAGE: {usage}\n"
                    f" --- {ctx.provider_name}  COST: {cost}"
                    f"{TEXT_COLOR_DEFAULT}"
                )

    list_pending_ids = ctx.list_pending_ids()
    g_logger.debug(f"There are {len(list_pending_ids)} remaining partitions to work on")

    # Worst-case overhead, in minutes, for initialization (negotiation, file transfer etc.)
    # TODO: make this dynamic, e.g. depending on the size of files to transfer
    # init_overhead = 3
    # Providers will not accept work if the timeout is outside of the [5 min, 30min] range.
    # We increase the lower bound to 6 min to account for the time needed for our demand to
    # reach the providers.
    # min_timeout, max_timeout = 6, 30
    # timeout = timedelta(
    #     minutes=max(
    #         min(init_overhead + len(list_pending_ids) * 2, max_timeout), min_timeout
    #     )
    # )
    timeout = timedelta(minutes=29)  # todo, make dynamic

    # sane defaults for cpu and dur per hr
    if payment_network == "rinkeby":
        max_price_for_cpu = Decimal("inf")
        max_price_for_dur = Decimal("inf")
    else:
        max_price_for_cpu = Decimal(MAX_PRICE_CPU_HR)
        max_price_for_dur = Decimal(MAX_PRICE_DUR_HR)

    strategy = yapapi.strategy.LeastExpensiveLinearPayuMS(
        max_fixed_price=Decimal("0.00"),
        max_price_for={
            yapapi.props.com.Counter.CPU: max_price_for_cpu / Decimal("3600.0"),
            yapapi.props.com.Counter.TIME: max_price_for_dur / Decimal("3600.0"),
        },
    )

    if moduleFilterProviderMS:
        strategy = FilterProviderMS(strategy)

    async with Golem(
        budget=10.0,
        subnet_tag=subnet_tag,
        payment_driver=payment_driver,
        payment_network=payment_network,
        strategy=strategy,
    ) as golem:
        print_env_info(golem)

        num_tasks = 0
        start_time = datetime.now()

        completed_tasks = golem.execute_tasks(
            worker,
            [MyTask(ctx, pending_id) for pending_id in list_pending_ids],
            payload=package,
            max_workers=max_workers,
            timeout=timeout,
        )
        async for task in completed_tasks:
            num_tasks += 1
            print(
                f"{TEXT_COLOR_CYAN}"
                f"Task computed: {task}, result: {task.result}, time: {task.running_time}"
                f"{TEXT_COLOR_DEFAULT}"
            )
            ctx.con.execute(
                "INSERT INTO Checksum(partId, hash) VALUES (?, ?)",
                (
                    task.data,
                    task.result["checksum"],
                ),
            )
            ctx.con.execute(
                "INSERT INTO OutputFile(partId, pathStr) VALUES (?, ?)",
                (
                    task.data,
                    task.result["path"],
                ),
            )
            ctx.con.commit()
        print(
            f"{TEXT_COLOR_CYAN}"
            f"{num_tasks} tasks computed, total time: {datetime.now() - start_time}"
            f"{TEXT_COLOR_DEFAULT}"
        )


if __name__ == "__main__":
    parser = build_parser("compress a file in parallel")
    parser.add_argument(
        "--show-usage",
        action="store_true",
        default=False,
        help="show activity usage and cost; default: %(default)s",
    )
    parser.add_argument(
        "--min-cpu-threads",
        type=int,
        default=0,
        help="require the provider nodes to have at least this number of available CPU threads",
    )
    parser.add_argument("--target", help="path to file to compress")
    parser.add_argument(
        "--divisions",
        type=int,
        default=10,
        help="Number partitions to distribute for invididual processing; default: %(default)d",
    )
    parser.add_argument(
        "--enable_logging", default=True, help="write log files; default: %(default)s"
    )
    parser.add_argument(
        "--compression",
        default="6e",
        help="compression from 0 fastest to 9 most compressed (optionally postfixed with"
        " e for extra cpu time); default: %(default)s",
    )
    parser.add_argument(
        "--xfer-compression-level",
        type=int,
        default="-1",
        help="compression from 0 to 9 locally before uploading to nodes (must be less than"
        " --compresssion), negative value implies no pre-compression (default)",
    )
    # now = datetime.now().strftime("%Y-%m-%d_%H.%M.%S")
    # parser.set_defaults(log_file=f"gompress-{now}.log")
    args = parser.parse_args()

    data_dir = Path("./workdir")
    data_dir.mkdir(exist_ok=True)
    target_file = Path(args.target)  # todo make an argument
    max_workers = args.divisions
    ctx = CTX(
        data_dir,
        target_file,
        max_workers,
        args.compression,
        args.xfer_compression_level,
        args.min_cpu_threads,
    )

    run_golem_example(
        main(
            ctx,
            subnet_tag=args.subnet_tag,
            min_cpu_threads=args.min_cpu_threads,
            payment_driver=args.payment_driver,
            payment_network=args.payment_network,
            show_usage=args.show_usage,
        ),
        log_file=args.log_file if args.enable_logging else None,
    )

    # confirm there exists a checksum for every partid
    # confirm the checksum matches each partid
    if ctx.verify():
        # concatenate output files in order of partid
        ctx.concatenate_and_finalize()
        print(f"The compressed file is located at: {ctx.path_to_final_target}")
    else:
        print("incomplete")
