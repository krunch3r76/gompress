#!/usr/bin/env python3
# gompress.py
# authored by krunch3r (https://www.github.com/krunch3r76)
# license GPL 3.0
# skeleton and utils adopted from Golem yapapi's code

from datetime import datetime, timedelta
import pathlib
import sys
from pathlib import Path, PurePath

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

from workdirectoryinfo import WorkDirectoryInfo
from ctx import CTX


class MyTask(Task):
    """Task extended with reference to context"""
    def __init__(self, mainctx, data):
        self.mainctx=mainctx
        super().__init__(data)


async def main(
    ctx, subnet_tag, min_cpu_threads, payment_driver=None, payment_network=None, show_usage=False
):
    list_pending_ids = ctx.list_pending_ids()
    package = await vm.repo(
        image_hash="79d62e635a201f07a243e69c60a7c18338d6a3a6a43b1154277a8a87",
        # only run on provider nodes that have more than 0.5gb of RAM available
        min_mem_gib=0.5,
        # only run on provider nodes that have more than 2gb of storage space available
        min_storage_gib=2.0,
        # only run on provider nodes which a certain number of CPU threads (logical CPU cores) available
        min_cpu_threads=min_cpu_threads,
    )
    async def worker(ctx: WorkContext, tasks):
        # Set timeout for the first script executed on the provider. Usually, 30 seconds
        # should be more than enough for computing a single frame of the provided scene,
        # however a provider may require more time for the first task if it needs to download
        # the VM image first. Once downloaded, the VM image will be cached and other tasks that use
        # that image will be computed faster.
        script = ctx.new_script(timeout=timedelta(minutes=10))

        async for task in tasks:
            partId = task.data # subclassed Task with id attribute
            # read range and write into temporary file
            view_to_temporary_file = task.mainctx.view_to_temporary_file(partId)
            # resolve to target
            path_to_remote_target = PurePath("/golem/workdir") / f"part_{partId}"
            # upload as resolved target
            # script.upload_file(str(path_to_temporary_file), str(path_to_remote_target))
            script.upload_bytes(view_to_temporary_file, str(path_to_remote_target))
            # run script on uploaded target
            future_result = script.run("/root/xz.sh", str(path_to_remote_target), "-T0", "-9"  )
            # resolve to processed target
            path_to_processed_target = PurePath(str(path_to_remote_target) + ".xz")

            local_output_file = task.mainctx.work_directory_info.path_to_parts_directory / path_to_processed_target.name
            script.download_file(path_to_processed_target, local_output_file)
            try:
                yield script
                # TODO: Check if job results are valid
                # and reject by: task.reject_task(reason = 'invalid file')
                result_dict = {}
                stdout = future_result.result().stdout
                if not stdout.startswith("OK:"):
                    task.reject_result(retry=True)
                    # this requires testing TODO
                else:
                    result_dict['checksum'] = stdout.split(':')[1][:-1]
                    result_dict['path'] = local_output_file.as_posix()
                    task.accept_result(result=result_dict)
            except BatchTimeoutError:
                print(
                    f"{TEXT_COLOR_RED}"
                    f"Task {task} timed out on {ctx.provider_name}, time: {task.running_time}"
                    f"{TEXT_COLOR_DEFAULT}"
                )
                raise

            # reinitialize the script which we send to the engine to compress subsequent parts
            script = ctx.new_script(timeout=timedelta(minutes=1))

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

    # Worst-case overhead, in minutes, for initialization (negotiation, file transfer etc.)
    # TODO: make this dynamic, e.g. depending on the size of files to transfer
    init_overhead = 3
    # Providers will not accept work if the timeout is outside of the [5 min, 30min] range.
    # We increase the lower bound to 6 min to account for the time needed for our demand to
    # reach the providers.
    min_timeout, max_timeout = 6, 30
    timeout = timedelta(minutes=max(min(init_overhead + len(list_pending_ids) * 2, max_timeout), min_timeout))


    async with Golem(
        budget=10.0,
        subnet_tag=subnet_tag,
        payment_driver=payment_driver,
        payment_network=payment_network,
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
            print(f"INSERTING CHECKSUM {task.data} {task.result}", flush=True)
            ctx.con.execute("INSERT INTO Checksum(partId, hash) VALUES (?, ?)",
                    (task.data, task.result["checksum"],)
                    )
            print("INSERTING OutputFile", flush=True)
            ctx.con.execute("INSERT INTO OutputFile(partId, pathStr) VALUES (?, ?)", (task.data, task.result["path"], ))
            ctx.con.commit()
        print(
            f"{TEXT_COLOR_CYAN}"
            f"{num_tasks} tasks computed, total time: {datetime.now() - start_time}"
            f"{TEXT_COLOR_DEFAULT}"
        )


if __name__ == "__main__":
    parser = build_parser("compress a file in parallel")
    parser.add_argument("--show-usage", action="store_true", help="show activity usage and cost")
    parser.add_argument(
        "--min-cpu-threads",
        type=int,
        default=1,
        help="require the provider nodes to have at least this number of available CPU threads",
    )
    now = datetime.now().strftime("%Y-%m-%d_%H.%M.%S")
    parser.set_defaults(log_file=f"gompress-{now}.log")
    args = parser.parse_args()

    data_dir = Path("/tmp/gompress_test")
    target_file = Path("/tmp/to_compress.tar") # todo make an argument
    max_workers=11
    ctx = CTX(data_dir, target_file, max_workers)

    run_golem_example(
        main(
            ctx,
            subnet_tag=args.subnet_tag,
            min_cpu_threads=args.min_cpu_threads,
            payment_driver=args.payment_driver,
            payment_network=args.payment_network,
            show_usage=args.show_usage,
        ),
        log_file=args.log_file,
    )

    # confirm there exists a checksum for every partid
    # confirm the checksum matches each partid
    if ctx.verify():
        print("ALL GOOD")
    else:
        print("incomplete")

    # concatenate output files in order of partid
    ctx.concatenate_and_finalize()
