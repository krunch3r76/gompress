#!/usr/bin/env python3
# gompress.py
"""
the requestor agent provisioning segments of a file for providers on the network to compress
"""

# authored by krunch3r (https://www.github.com/krunch3r76)
# license GPL 3.0
# skeleton and utils adopted from Golem yapapi's code


MAX_PRICE_CPU_HR = "0.019"
MAX_PRICE_DUR_HR = "0.0"
MAX_MINUTES_UNTIL_TASK_IS_A_FAILURE = 15
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
    """Task class extended to store a reference to the caller (ctx)"""

    def __init__(self, mainctx, data):
        self.mainctx = mainctx
        super().__init__(data)


def find_optimal_xz_preset(file_length):
    """map a file_length to the xz dictionary size that first does not exceed it
    and return corresponding compression argument


    :param file_length: length of the file (part) to compress


    rationale: it is a waste of memory to use a dictionary size bigger than the
    uncompressed file. this may imply less complexity depending on how xz implements.
    """

    KiB = 2**10
    MiB = 2**20

    if file_length < 256 * KiB:
        return "-0e"
    elif file_length < 2 * MiB:
        return "-1e"
    elif file_length < 4 * MiB:
        return "-2e"
    elif file_length < 8 * MiB:
        return "-4e"
    elif file_length < 16 * MiB:
        return "-6e"
    elif file_length < 32 * MiB:
        return "-7e"
    elif file_length < 64 * MiB:
        return "-8e"
    else:
        return "-9e"


async def main(
    ctx,
    subnet_tag,
    min_cpu_threads,
    payment_driver,
    payment_network,
    show_usage,
):
    """partition input target file into segments of 64MiB and task to compress across golem nodes

    :param ctx: the class with contextual information useful to workers
    :param subnet_tag: provided as a cli argument
    :param min_cpu_threads: the thread count beneath which provider offers are rejected
        which is provided as a cli argument (only useful to eliminate unwanted providers
        but may important if segmentation is >=128 MiB per task in the future)
    :param payment_network: provided as a cli argument
    :param show_usage: provided as a cli argument


    """

    # identify vm that tasked nodes are to use to process the payload/instructions
    package = await vm.repo(
        image_hash="8680582af7665463e0c79ceadf72f8d82643b973108c4a8fc1bb65af",
        # only run on provider nodes that have more than 1.0gb of RAM available
        min_mem_gib=1.0,  # later set this to 1.5 when 128mb divisions allowed
        # only run on provider nodes that have more than 2gb of storage space available
        min_storage_gib=2.0,  # this is more than enough for a 64-128 mb segment
        # since the work is mostly done in memory
        # only run on provider nodes which a certain number of CPU threads (logical CPU cores) available
        min_cpu_threads=min_cpu_threads,
    )

    async def worker(ctx: WorkContext, tasks):
        """refers to the task data to lookup the range of bytes to work on

        :param ctx: Provider node's work context (distinguised from gompress ctx)
        :param tasks: iterable to pending work

        a worker is associated with one and only one provider at a time via WorkContext
        """

        g_logger.debug(f"working: {ctx}")
        # Set timeout for the first script/task to be executed on the provider given
        # the task iterator
        script = ctx.new_script(
            timeout=timedelta(minutes=MAX_MINUTES_UNTIL_TASK_IS_A_FAILURE)
        )

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
                # review, upload_bytes requires len() so intermediary may not make sense
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

            optimal_compression_argument = find_optimal_xz_preset(
                task.mainctx.path_to_target.stat().st_size
            )

            # because we are partitioning according to the maximum dictionary size
            # it would impose geometrically escalated memory requirements per thread
            # without additional compression effectiveness to use more than one thread
            # per 64 MiB (current segmentation as of this writing). Therefore, we
            # pass -T1 to xz
            future_result = script.run(
                "/root/xz.sh",
                path_to_remote_target.name,  # shell script is run from workdir, expects
                # filename is local to workdir
                # f"-T{task.mainctx.min_threads}",
                f"-T1",  # in the future, for 128 parts
                # , utilize at most 2 threads corresponding to 2 64MiB
                f"{optimal_compression_argument}",
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

            # reinitialize the script for the next task if any (partition to compress)
            script = ctx.new_script(
                timeout=timedelta(minutes=MAX_MINUTES_UNTIL_TASK_IS_A_FAILURE)
            )

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
    init_overhead = 3
    # Providers will not accept work if the timeout is outside of the [5 min, 30min] range.
    # We increase the lower bound to 6 min to account for the time needed for our file to
    # reach the providers.
    min_timeout, max_timeout = 6, 30
    timeout = timedelta(
        minutes=max(
            min(init_overhead + len(list_pending_ids) * 2, max_timeout), min_timeout
        )
    )

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

    def emitter(event):
        """sniff events before they reach the SummaryLogger on Golem

        :param event: see reference to common event attributes and event types
        reference: https://github.com/golemfactory/yapapi/blob
                         /a1-reputation-prototype/yapapi/events.py
        """

        # if isinstance(event, yapapi.events.ProposalReceived)
        event_name = event.__class__.__name__
        if "Proposal" not in event_name and "DebitNote" not in event_name:
            # g_logger.debug(f"\t\t{event}")
            try:
                if "SendBytes" in event.commands:
                    pass
            except:
                pass

    async with Golem(
        budget=10.0,
        subnet_tag=subnet_tag,
        payment_driver=payment_driver,
        payment_network=payment_network,
        strategy=strategy,
        event_consumer=yapapi.log.SummaryLogger(emitter).log,
    ) as golem:

        print_env_info(golem)

        num_tasks = 0
        start_time = datetime.now()

        completed_tasks = golem.execute_tasks(
            worker,
            [MyTask(ctx, pending_id) for pending_id in list_pending_ids],
            payload=package,
            max_workers=ctx.part_count,
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


def add_arguments_to_command_line_parser():
    """build command line parser arguments and parse the arguments returning parser object"""
    #########################
    # pull in a template    #
    #########################
    parser = build_parser("compress a file in parallel tasked to golem providers")

    #########################
    # add arguments         #
    #########################
    parser.add_argument("target", help="path to file to compress")
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
    # parser.add_argument("--target", help="path to file to compress")
    parser.add_argument(
        "--enable_logging", default=True, help="write log files; default: %(default)s"
    )

    parser.add_argument(
        "--xfer-compression-level",
        type=int,
        default="-1",
        help="compression from 0 to 9 locally before uploading to nodes (must be less than"
        " --compresssion), negative value implies no pre-compression (default)",
    )

    return parser


if __name__ == "__main__":
    # now = datetime.now().strftime("%Y-%m-%d_%H.%M.%S")
    # parser.set_defaults(log_file=f"gompress-{now}.log")

    parser = add_arguments_to_command_line_parser()
    args = parser.parse_args()
    target_file = Path(args.target)
    data_dir = Path("./workdir")
    data_dir.mkdir(exist_ok=True)

    ctx = CTX(
        data_dir,
        target_file,
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

    if ctx.verify():
        # concatenate output files in order of partid
        ctx.concatenate_and_finalize()
        print(f"The compressed file is located at: {ctx.path_to_final_target}")
    else:
        print("incomplete")
