from workdirectoryinfo import WorkDirectoryInfo, sha1_hash
import os
import sqlite3
from _create_connection import create_connection
import io
from pathlib import Path

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

    def __init__(self, path_to_rootdir_in, path_to_target_in, part_count_in):
        """create/connect to a db relevant to inputs"""
        self.path_to_target = path_to_target_in
        self.target_open_file = self.path_to_target.open('rb')
        self.path_to_rootdir = path_to_rootdir_in
        self.part_count = part_count_in
        self.work_directory_info = WorkDirectoryInfo(self.path_to_rootdir, self.path_to_target)
        # check if work.db exists in work_directory_info.path_to_target_wdir
        self.path_to_connection_file = self.work_directory_info.path_to_target_wdir / "work.db"
        if self.path_to_connection_file.exists():
            self.con = sqlite3.connect(str(self.path_to_connection_file), isolation_level=None)
            # TODO: check for completeness
        else:
            # create connection
            print("creating connection")
            self.con = create_connection(self.path_to_connection_file, self.path_to_target,
                    self.work_directory_info, self.part_count)

    def view_to_temporary_file(self, partId):
        """lookup the range on the connection and return as a BytesIO object"""
        record = self.con.execute(f"SELECT start, end FROM Part WHERE partId = {partId}").fetchone()
        read_range = (record[0], record[1])
        self.target_open_file.seek(read_range[0])
        bytesIO = io.BytesIO(self.target_open_file.read(read_range[1]-read_range[0]))
        return bytesIO.getvalue() # bytesIO is cleaned up only when view is destroyed...
        # return bytesIO.getbuffer() # bytesIO is cleaned up only when view is destroyed...

    def list_pending_ids(self):
        """return a list or iterable of partIds for which there is no checksum"""
        pending_id_list = self.con.execute("SELECT partId FROM Part WHERE partId NOT IN (SELECT partId FROM Checksum)").fetchall()
        return [ pending_id_row[0] for pending_id_row in pending_id_list ]

    def verify(self):
        """return True if all parts match downloaded and match checksums"""
        if len(self.list_pending_ids()) != 0:
            return False

        recordset = self.con.execute("SELECT pathStr, hash, partId FROM OutputFile NATURAL JOIN Checksum ORDER BY partId").fetchall()
        OK = True
        PATH_FIELD_OFFSET=0
        HASH_FIELD_OFFSET=1
        PARTID_FIELD_OFFSET=2
        for record in recordset:
            print(f"verifying part 1 thru {record[PARTID_FIELD_OFFSET]}...", end="")
            path_to_part = Path(record[PATH_FIELD_OFFSET])
            if not path_to_part.exists():
                OK = False
                print(f"part {record[PARTID_FIELD_OFFSET]} NOT FOUND!")
                break
            part_hash = sha1_hash(str(path_to_part))
            if part_hash != record[HASH_FIELD_OFFSET]:
                OK = False
                print(f"part {record[PARTID_FIELD_OFFSET]} BAD")
                break
            else:
                print("OK", end="\r")
        print("\n")
        return OK

    def concatenate_and_finalize(self):
        """using the first part append and delete subsequent parts and rename to original
        suffixed with .xz"""
        recordset = self.con.execute("SELECT pathStr from OutputFile ORDER BY partId").fetchall()
        PATHSTR_FIELD_OFFSET=0
        paths = [ Path(record[PATHSTR_FIELD_OFFSET]) for record in recordset ]
        # open first part for appending
        path_to_first = paths.pop(0)
        with open(str(path_to_first), "ab") as concat:
            for path in paths:
                with open(str(path), "rb") as to_concat:
                    concat.write(to_concat.read())
                path.unlink()
        name_of_final_file = self.path_to_target.name + ".xz"
        # print(f"name of final file will be {name_of_final_file}")
        # path_to_first.rename(name_of_final_file)
        path_to_final_target = self.work_directory_info.path_to_final_directory / name_of_final_file
        path_to_first.rename(path_to_final_target)

