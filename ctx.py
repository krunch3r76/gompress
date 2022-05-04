import os
import sqlite3
import io
from pathlib import Path
from datetime import timedelta

from workdirectoryinfo import WorkDirectoryInfo, checksum
from _create_connection import create_connection, _partition
from debug.mylogging import g_logger


class CTX:
    """
    an interface to the model to track/finalize workdir files and hold run parameters

    ---------------------------
    min_threads                 minimum threads we expect from a provider
    precompression_level        0-9 (compression level of bytes in memory before upload) or -1
    path_to_target              the file to be compressed
    / target_open_file            file object wrapping target file
    / name_of_final_file        the name to which the compressed result will be stored
    / path_to_final_file        Path object to final file
    / part_count                the total number of divisions of the target file worked on
    path_to_local_workdir       Path to local working directory
    / work_directory_info         WorkDirectoryInfo object containing information about the working dir
    / path_to_connection_file     the database containing information about the work to be done
    con                         connection to the database (path_to_connection_file)
    total_vm_run_time           updated with cumulative vm run times
    ---------------------------
    concatenate_and_finalize()  merge downloaded parts
    list_pending_ids()          check the connection to identify any missing parts
    reset_workdir()             clear pending work, from tables, (and files if applicable)
    verify()                    ensure checksums match what was told by the provider
    view_to_temporary_file()    get a memory view of a part of the file to be worked on
    lookup_partition_range()    get the range [beg, end) for a specific division
    len_file()                  return the size of the file {target, final}
    """

    def __init__(
        self,
        path_to_local_workdir_in,
        path_to_target_in,
        precompression_level_in,
        min_threads_in,
    ):
        """initialize the context

        :param path_to_local_workdir_in:    Path to the main work directory to put/lookup work in subs
        :param path_to_target_in:           Path to the file to compress
        :param precompression_level_in:     level of compression to use in memory before uploading (-1 none)
        :param min_theads_in:               minimum number of threads a provider should have to be used

        """

        ###############################
        # assign input attributes     #
        ###############################
        self.min_threads = min_threads_in
        self.precompression_level = precompression_level_in
        self.path_to_target = path_to_target_in
        self.path_to_local_workdir = path_to_local_workdir_in

        ###############################
        # assign computed properties  #
        ###############################
        self.total_vm_run_time = timedelta()
        self.work_directory_info = WorkDirectoryInfo(
            self.path_to_local_workdir, self.path_to_target
        )
        self.name_of_final_file = self.path_to_target.name + ".xz"
        self.path_to_final_file = (
            self.work_directory_info.path_to_final_directory / self.name_of_final_file
        )
        self.target_open_file = self.path_to_target.open("rb")
        self.part_count = len(_partition(self.path_to_target.stat().st_size, None))
        self.path_to_connection_file = (
            self.work_directory_info.path_to_target_wdir / "work.db"
        )

        # --------- create_new_connection() -------------
        def create_new_connection(self):
            g_logger.debug("creating connection")
            self.con = create_connection(
                self.path_to_connection_file,
                self.path_to_target,
                self.work_directory_info,
            )

        #########################
        # create new connection #
        #########################
        new_connection = None
        if not self.path_to_connection_file.exists():
            create_new_connection(self)
        else:
            ###########################
            # use existing connection #
            ###########################
            new_connection = False  # may become true by end
            self.con = sqlite3.connect(
                str(self.path_to_connection_file), isolation_level=None
            )
            last_part_count = self.con.execute(
                "SELECT part_count FROM OriginalFile"
            ).fetchone()[0]

            g_logger.debug(f"parts remaining: {last_part_count}")

            if last_part_count != self.part_count:
                ##############################
                # ! overwrite bad connection #
                ##############################
                g_logger.debug(
                    f"the part count differs from the last work which was for {self.part_count}!"
                )
                self.con.close()
                self.path_to_connection_file.unlink()
                new_connection = True
                create_new_conenction(self)

        if self.path_to_final_file.exists():
            raise Exception(
                f"There appears to be a compressed file already for this at {self.path_to_final_file}"
            )
            self.path_to_final_file.unlink()

    def lookup_partition_range(self, partId):
        """get the range [beg, end) for a specific division"""
        record = self.con.execute(
            f"SELECT start, end FROM Part WHERE partId = {partId}"
        ).fetchone()
        read_range = (
            record[0],
            record[1],
        )
        return read_range

    def view_to_temporary_file(self, partId):
        """get a memory view of a part of the file to be worked on"""

        # record = self.con.execute(
        #     f"SELECT start, end FROM Part WHERE partId = {partId}"
        # ).fetchone()
        # read_range = (record[0], record[1])
        read_range = self.lookup_partition_range(partId)
        self.target_open_file.seek(read_range[0])
        bytesIO = io.BytesIO(self.target_open_file.read(read_range[1] - read_range[0]))
        # return bytesIO.getvalue()
        return (
            bytesIO.getbuffer()
        )  # bytesIO is cleaned up only when view is destroyed...

    def list_pending_ids(self):
        """check the connection to identify any missing parts"""

        pending_id_list = self.con.execute(
            "SELECT partId FROM Part WHERE partId NOT IN (SELECT partId FROM Checksum)"
        ).fetchall()
        list_of_pending_ids = [pending_id_row[0] for pending_id_row in pending_id_list]
        # g_logger.debug(f"There are {len(list_of_pending_ids)} partitions to work on")
        return list_of_pending_ids

    def verify(self):
        """ensure checksums match what was told by the provider"""

        if len(self.list_pending_ids()) != 0:
            return False  # need all parts to verify

        recordset = self.con.execute(
            "SELECT pathStr, hash, partId FROM OutputFile NATURAL JOIN Checksum ORDER BY partId"
        ).fetchall()
        OK = True
        PATH_FIELD_OFFSET = 0
        HASH_FIELD_OFFSET = 1
        PARTID_FIELD_OFFSET = 2
        for record in recordset:
            print(f"verifying part 1 thru {record[PARTID_FIELD_OFFSET]}...", end="")
            path_to_part = Path(record[PATH_FIELD_OFFSET])
            if not path_to_part.exists():
                OK = False
                print(f"part {record[PARTID_FIELD_OFFSET]} NOT FOUND!")
                break
            part_hash = checksum(path_to_part)
            if part_hash != record[HASH_FIELD_OFFSET]:
                OK = False
                print(f"part {record[PARTID_FIELD_OFFSET]} BAD")
                break
            else:
                print("\033[32m\u2713\033]0m", end="\r")
        print("\n")
        return OK

    def concatenate_and_finalize(self):
        """merge downloaded parts"""

        recordset = self.con.execute(
            "SELECT pathStr from OutputFile ORDER BY partId"
        ).fetchall()
        PATHSTR_FIELD_OFFSET = 0
        paths = [Path(record[PATHSTR_FIELD_OFFSET]) for record in recordset]
        g_logger.debug(paths)
        # open first part for appending
        path_to_first = paths.pop(0)
        with open(str(path_to_first), "ab") as concat:
            for path in paths:
                g_logger.debug(f"concatenating {path} with {path_to_first}")
                with open(str(path), "rb") as to_concat:
                    concat.write(to_concat.read())
        path_to_first.rename(self.path_to_final_file)
        self.reset_workdir(keep_final=True)

    def reset_workdir(self, keep_final=False):
        """clear parts and associated sql records"""

        files_recordset = self.con.execute("SELECT pathStr FROM OutputFile").fetchall()
        for path_to_output_file in [Path(row[0]) for row in files_recordset]:
            if path_to_output_file.exists():
                path_to_output_file.unlink()
        self.con.execute(
            "DELETE FROM Checksum"
        )  # no checksum can exist now that parts are gone
        self.con.execute(
            "DELETE FROM OutputFile"
        )  # no outputfile can exist now that parts are gone
        if not keep_final:
            if self.path_to_final_file.exists():
                self.path_to_final_file.unlink()

    def len_file(self, target=True):
        """return length of target (default) or final file"""
        filelen_rv = None
        if target:
            filelen_rv = self.path_to_target.stat().st_size
        else:
            if self.path_to_final_file.exists():
                filelen_rv = self.path_to_final_file.stat().st_size
            else:
                raise Exception(
                    "Cannot query length of final final, it does not exist!"
                )
        return filelen_rv
