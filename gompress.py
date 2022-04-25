#!/usr/bin/env python3
# gompress.py
# authored by krunch3r (https://www.github.com/krunch3r76)
# license GPL 3.0
# skeleton and utils adopted from Golem yapapi's code

from datetime import datetime, timedelta
import pathlib
import sys
from pathlib import Path, PurePosixPath
from debug.mylogging import g_logger

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
        image_hash="682edc860a5742b800f90b14c85ea88b08e44cccb127ccb5a5f1f13b",
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
            path_to_remote_target = PurePosixPath("/golem/workdir") / f"part_{partId}"
            script.upload_bytes(
                view_to_temporary_file.tobytes(), str(path_to_remote_target)
            )

            # run script on uploaded target
            future_result = script.run(
                "/root/xz.sh",
                str(path_to_remote_target),
                "-T0",
                f"-{task.mainctx.compression_level}",
            )
            # resolve to processed target
            path_to_processed_target = PurePosixPath(str(path_to_remote_target) + ".xz")
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
    init_overhead = 3
    # Providers will not accept work if the timeout is outside of the [5 min, 30min] range.
    # We increase the lower bound to 6 min to account for the time needed for our demand to
    # reach the providers.
    min_timeout, max_timeout = 6, 30
    timeout = timedelta(
        minutes=max(
            min(init_overhead + len(list_pending_ids) * 2, max_timeout), min_timeout
        )
    )

    if moduleFilterProviderMS:
        strategy = FilterProviderMS()
    else:
        strategy = None

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
        default=1,
        help="require the provider nodes to have at least this number of available CPU threads",
    )
    parser.add_argument("--target", help="path to file to compress")
    parser.add_argument(
        "--divisions",
        type=int,
        default=20,
        help="Number partitions to distribute for invididual processing; default: %(default)d",
    )
    parser.add_argument(
        "--enable_logging", default=True, help="write log files; default: %(default)s"
    )
    parser.add_argument(
        "--compression",
        default="6e",
        help="compression from 1 fastest to 9 most compressed (optionally postfixed with e for extra cpu time); default: %(default)s",
    )
    # now = datetime.now().strftime("%Y-%m-%d_%H.%M.%S")
    # parser.set_defaults(log_file=f"gompress-{now}.log")
    args = parser.parse_args()

    data_dir = Path("./workdir")
    data_dir.mkdir(exist_ok=True)
    target_file = Path(args.target)  # todo make an argument
    max_workers = args.divisions
    ctx = CTX(data_dir, target_file, max_workers, args.compression)

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
