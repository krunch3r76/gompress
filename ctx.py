from workdirectoryinfo import WorkDirectoryInfo, checksum
import os
import sqlite3
from _create_connection import create_connection, _partition
import io
from pathlib import Path
from debug.mylogging import g_logger


class CTX:
    """
    path_to_rootdir                 <rootdir>
    path_to_target                  ?
    part_count                      n
    work_directory_info             <rootdir>/...
    path_to_connection_file         <rootdir>/work.db
    con                             keeping track of work to do/done
    target_open_file                target file as an open binary stream
    view_to_temporary_file(...)     returns a the range of bytes corresponding to id
    list_pending_ids()              returns a list of any id for which no part has a checksum
    verify(...)                     checks that all parts exist and match checksums
    """

    def __init__(
        self,
        path_to_rootdir_in,
        path_to_target_in,
        # part_count_in,
        # compression_level_in,
        precompression_level_in,
        min_threads_in,
    ):
        """create/connect to a db relevant to inputs"""
        self.min_threads = min_threads_in
        self.precompression_level = precompression_level_in
        # self.compression_level = compression_level_in
        self.path_to_target = path_to_target_in
        self.target_open_file = self.path_to_target.open("rb")
        self.path_to_rootdir = path_to_rootdir_in
        self.part_count = len(_partition(self.path_to_target.stat().st_size, None))
        self.work_directory_info = WorkDirectoryInfo(
            self.path_to_rootdir, self.path_to_target
        )
        self.work_has_been_reset = False
        # check if work.db exists in work_directory_info.path_to_target_wdir
        self.path_to_connection_file = (
            self.work_directory_info.path_to_target_wdir / "work.db"
        )

        self.name_of_final_file = self.path_to_target.name + ".xz"
        self.path_to_final_target = (
            self.work_directory_info.path_to_final_directory / self.name_of_final_file
        )
        # print(f"looking for {self.path_to_final_target}")

        new_connection = True
        if self.path_to_connection_file.exists():
            new_connection = False  # may become true by end
            self.con = sqlite3.connect(
                str(self.path_to_connection_file), isolation_level=None
            )
            # TODO: check for completeness
            last_part_count = self.con.execute(
                "SELECT part_count FROM OriginalFile"
            ).fetchone()[0]

            g_logger.debug(f"last part count: {last_part_count}")

            if last_part_count != self.part_count:
                g_logger.debug(
                    f"the part count differs from the last work which was for {self.part_count}!"
                )
                self.con.close()
                self.path_to_connection_file.unlink()
                new_connection = True
                self.work_has_been_reset = True

        if new_connection:
            # create connection
            g_logger.debug("creating connection")
            self.con = create_connection(
                self.path_to_connection_file,
                self.path_to_target,
                self.work_directory_info,
            )

        if self.path_to_final_target.exists():
            if not self.work_has_been_reset:
                raise Exception(
                    f"There appears to be a compressed file already for this at {self.path_to_final_target}"
                )
            self.path_to_final_target.unlink()
            self.work_has_been_reset = False

    def view_to_temporary_file(self, partId):
        """lookup the range on the connection and return as a BytesIO object"""
        record = self.con.execute(
            f"SELECT start, end FROM Part WHERE partId = {partId}"
        ).fetchone()
        read_range = (record[0], record[1])
        self.target_open_file.seek(read_range[0])
        bytesIO = io.BytesIO(self.target_open_file.read(read_range[1] - read_range[0]))
        # return bytesIO.getvalue()
        return (
            bytesIO.getbuffer()
        )  # bytesIO is cleaned up only when view is destroyed...

    def list_pending_ids(self):
        """return a list or iterable of partIds for which there is no checksum"""
        pending_id_list = self.con.execute(
            "SELECT partId FROM Part WHERE partId NOT IN (SELECT partId FROM Checksum)"
        ).fetchall()
        list_of_pending_ids = [pending_id_row[0] for pending_id_row in pending_id_list]
        # g_logger.debug(f"There are {len(list_of_pending_ids)} partitions to work on")
        return list_of_pending_ids

    def verify(self):
        """look up paths to output file and corresponding checksums and return whether
        every file checksum matches what the provider reported"""

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
                print("OK", end="\r")
        print("\n")
        return OK

    def concatenate_and_finalize(self):
        """merge all the downloaded parts in order according to their suffix

        beginning with the first part append and delete subsequent parts
        finally, rename the first part to original name of target suffixed with .xz
        """

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
                # path.unlink()
        path_to_first.rename(self.path_to_final_target)
        self.reset_workdir()

    def reset_workdir(self, pending=True):
        """clear parts and associated sql records"""
        if pending == False:
            raise Exception("pending False not implemented")

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
        self.work_has_been_reset = True
