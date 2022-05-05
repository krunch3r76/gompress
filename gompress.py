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
START_PRICE = "0.0"

from datetime import datetime, timedelta

MAX_MINUTES_UNTIL_TASK_IS_A_FAILURE = 5
MAX_TIMEOUT_FOR_TASK = timedelta(minutes=MAX_MINUTES_UNTIL_TASK_IS_A_FAILURE)

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

import random

random.seed()

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

    gompress partitions the target file into lengths of 64MiB sending each as a block
    for a distinct node to work on. min_cpu_threads may be used to select providers
    with more modern cpu's, but does not affect compression effectiveness as a single
    thread is utilized for each block. this model makes optimal use of memory which
    otherwise geometrically rises per core without any additional benefit. therefore,
    gompress essentially improves xz by requiring less memory for parallel compression.

    each task is given a shared context object and a unique part number representing
    which sequential part of the whole file it shall work on.
    """

    # identify vm that tasked nodes are to use to process the payload/instructions
    package = await vm.repo(
        image_hash="8f2396e5a50c206e5eb671816f67976841007e6d759511cb552c4b3e",
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
        a worker that has gained access to a given node reads the range of bytes from
        the file to compress given the part offset on the task's data property. it does
        so via a query of the database table stored in the workdir.
        a worker may compress the bytes in memory before uploaded as per client command
        line arguments.
        a remote script on the vm is invoked after the file has been uploaded to compress.
        the worker downloads the result and places it in the local workdir.
        the worker records the stdout to capture the checksum, which is the length of
        the file by default. error checking is expected to occur on the transport level
        so a successful transfer is one in which all expected bytes were received.
        the worker then moves on to the next task (part of file needing compression) if any
        not already assigned elsewhere.
        a worker may disconnect from the provider if it is taking too long, as per the (global)
        variable MAX_MINUTES_UNTIL_TASK_IS_A_FAILURE. the executor then invokes worker on
        the next available "worker" i.e. provider. note: max workers is computed per run
        based on how many divisions of 64MiB there are.
        """

        def walltime_to_timedelta(walltime: str):
            walltime_split_on_m = walltime.split("m")
            minutes_str = walltime_split_on_m[0]
            seconds_fract_str = walltime_split_on_m[1][:-1].strip()
            return timedelta(minutes=int(minutes_str), seconds=float(seconds_fract_str))

        g_logger.debug(f"working: {ctx}")
        # Set timeout for the first script/task to be executed on the provider given
        # the task iterator
        # this can probably be moved to the head of async for below so as to not repeat it at loop end
        script = ctx.new_script(timeout=MAX_TIMEOUT_FOR_TASK)

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
                f"-T1",  # in the future, for 128 parts
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
                if not stdout.startswith("OK"):
                    task.reject_result(retry=True)
                    print(f"\033[1mrejected a result {stdout} and retrying\033[0m")
                    # try on deliberate rejection requires testing TODO
                else:

                    outputs = stdout.split("---")
                    outputs = list(
                        map(lambda s: s.strip(), outputs),
                    )

                    model = outputs.pop(len(outputs) - 1)
                    ######################################################
                    # reduce consecutive spaces in model to single space #
                    # https://stackoverflow.com/a/30517392               #
                    ######################################################
                    model_spaces_split = model.split(" ")
                    model_cleaned = filter(None, model_spaces_split)
                    model = " ".join(model_cleaned)
                    g_logger.debug(outputs)

                    ####################################################
                    # store info from stdout into a dictionary result  #
                    ####################################################
                    result_dict["checksum"] = outputs[1]
                    result_dict["walltime"] = walltime_to_timedelta(outputs[2])
                    result_dict["path"] = str(local_output_file.as_posix())
                    result_dict["model"] = model
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
            # TODO catch activity terminated by provider..
            except Exception as e:
                print(
                    f"\033[1;33ma worker experienced an unhandled exception:\033[0m{e}"
                )
                task.reject_result(retry=True)  # testing
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
    min_timeout, max_timeout = 10, 30
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
        max_fixed_price=Decimal(START_PRICE),
        max_price_for={
            yapapi.props.com.Counter.CPU: max_price_for_cpu / Decimal("3600.0"),
            yapapi.props.com.Counter.TIME: max_price_for_dur / Decimal("3600.0"),
        },
    )

    # if gc__filterms has been successfully imported, wrap the strategy
    if moduleFilterProviderMS:
        strategy = FilterProviderMS(strategy)

    # ----------------------------------------------
    # --------------- emitter() --------------------
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

    # interface with the payload (package defined aboved) to partition work
    # to providers
    async with Golem(
        budget=10.0,
        subnet_tag=subnet_tag,
        payment_driver=payment_driver,
        payment_network=payment_network,
        strategy=strategy,
        event_consumer=yapapi.log.SummaryLogger(emitter).log,
    ) as golem:

        # show client the network options being used, e.g. subnet-tag
        print_env_info(golem)

        if payment_network != "rinkeby":
            print(
                f"Using max cpu/hr: \033[1;33m{MAX_PRICE_CPU_HR}\033[0m;"
                f" max duration/hr: \033[1;33m{MAX_PRICE_DUR_HR}\033[0m;"
                f" and fixed start rate: \033[1;33m{START_PRICE}\033[0m"
                f" {'t' if payment_network == 'rinkeby' else ''}\033[1mGLM\033[0m"
            )

        print(f"The job's max timeout has been set to {timeout}")
        print(f"A task will be retried after a timeout of {MAX_TIMEOUT_FOR_TASK}\n")

        if ctx.whether_resuming:
            pendingCount = len(ctx.list_pending_ids())
            print(
                f"\033[1mResuming an earlier session to compress `{ctx.path_to_target.name}` of which"
                f" {pendingCount} part{'s' if pendingCount > 1 else ''}"
                f" remain{'' if pendingCount > 1 else 's'} out of {ctx.part_count}.\033[0m"
            )
        else:
            print(
                "\033[1m"
                f"Beginning new session and compressing `{ctx.path_to_target.name}`"
                f" in {ctx.part_count} task parts."
                "\033[0m"
            )

        num_tasks = 0
        start_time = datetime.now()

        completed_tasks = golem.execute_tasks(
            worker,
            [MyTask(ctx, pending_id) for pending_id in list_pending_ids],
            payload=package,
            max_workers=ctx.part_count,
            timeout=timeout,
        )
        # submit and asynchronous wait on the completed tasks
        # holding a reference to the database model with partition information
        # and update the database with the result information about the download
        # note, all tasks that have come back are expected to not be in rejected state
        # i.e. the worker will retry and not return a bad one
        async for task in completed_tasks:
            num_tasks += 1
            ctx.total_vm_run_time += task.result["walltime"]
            g_logger.debug(task.result)
            original_range = ctx.lookup_partition_range(task.data)
            original_length = original_range[1] - original_range[0]
            original_length_mib = original_length / 2**20
            compressed_length_mib = int(task.result["checksum"]) / 2**20
            print(
                f"{TEXT_COLOR_CYAN}"
                f"Task computed: {task},"
                f" {original_length_mib:,.{2}f}MiB \u2192 {compressed_length_mib:,.{2}f}MiB,"
                f" xz: {str(task.result['walltime'])[:-4]},"
                f" task: {str(task.running_time)[:-4]},"
                f" on an {task.result['model']}"
                f"{TEXT_COLOR_DEFAULT}"
            )
            #####################################
            # record length (checksum) in model #
            #####################################
            ctx.con.execute(
                "INSERT INTO Checksum(partId, hash) VALUES (?, ?)",
                (
                    task.data,
                    task.result["checksum"],
                ),
            )
            ###########################################
            # record path to downloaded part in model #
            ###########################################
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

    ################################################
    # create object to store information about run #
    ################################################
    ctx = CTX(
        data_dir,
        target_file,
        args.xfer_compression_level,
        args.min_cpu_threads,
    )

    #####################
    #      run          #
    #####################
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

    #####################
    #       verify      #
    #####################
    if ctx.verify():
        ######################
        #    concatenate     #
        ######################
        ctx.concatenate_and_finalize()

        #################
        #    report     #
        #################
        original_mib = ctx.len_file(target=True) / 2**20
        final_mib = ctx.len_file(target=False) / 2**20

        def exclamation():
            exclamations = ["wow!", "wowowowowow!", "w0w!", "w0w0w0w0w0w0w!"]
            return random.choice(exclamations)

        print(
            f"The run was a success! \033[1m{ctx.path_to_target.name}\033[0m has been compressed"
            f" to {final_mib:,.{2}f}MiB from {original_mib:,.{2}f}MiB",
            end="",
        )
        if final_mib / original_mib < 0.330001:
            print(",", exclamation())
        else:
            print(".")

        if not ctx.whether_resuming:
            print(
                f"The time spent on compressing the data with xz was clocked at"
                f" {str(ctx.total_vm_run_time)[:-4]}."
            )
        print(
            f"You can find the compressed file at"
            f" \033[1;33m{ctx.path_to_final_file}\033[0m"
        )
    else:
        countPending = len(ctx.list_pending_ids())
        print(
            f"\033[1;31mthe run did not finish, please re-run to compress the"
            f" remaining {countPending} part{'s' if countPending > 1 else ''}.\033[0m"
        )
        print(
            "\033[1m"
            "As always, on behalf on the golem community, thank you for your participation"
            "\033[0m"
        )
