import sqlite3
from debug.mylogging import g_logger

# maxcount being deprecated
def _partition(total, maxsize=None):
    """return an array of end lengths measuring maxsize evenly
    at least n-1 times"""
    if maxsize == None:
        maxsize = 64 * 2**20
    rv = []
    measure_count = total // maxsize
    if measure_count == 0:
        rv.append(total)
    else:
        for i in range(measure_count):
            rv.append(maxsize * (i + 1))
        extra = total % maxsize
        if extra != 0:
            rv.append(rv[-1] + extra)
    return rv


def _partitionRanges(target_length):
    # lengths = _partition(target_length, part_count)
    offsets = _partition(target_length)
    g_logger.debug(offsets)
    ranges = [
        (
            0,
            offsets.pop(0),
        )
    ]
    # lengths.pop(0)
    for i, offset in enumerate(offsets, 1):
        next_range = (ranges[i - 1][1], offset)
        ranges.append(next_range)

    g_logger.debug(ranges)

    return ranges


def _populate_connection(con, target_length, workDirectoryInfo):
    """build meta details and add to database"""

    ranges = _partitionRanges(target_length)

    con.execute(
        """
            INSERT INTO OriginalFile(file_hash, part_count) VALUES (?,?)""",
        (
            workDirectoryInfo.path_to_target_wdir.name,
            len(ranges),
        ),
    )

    con.executemany("INSERT INTO Part(start, end) VALUES (?,?)", ranges)


def create_connection(path_to_connection_file, path_to_target, workDirectoryInfo):
    """create a new database and return the connection"""
    # part_count = int(part_count) # kludge, should be guaranteed as pre
    workDirectoryInfo.create_skeleton()

    con = sqlite3.connect(str(path_to_connection_file), isolation_level=None)
    con.execute(
        """
        CREATE TABLE OriginalFile(
            originalFileId INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
            file_hash TEXT NOT NULL,
            part_count INTEGER NOT NULL
        )"""
    )

    con.execute(
        """
        CREATE TABLE Part(
            partId INTEGER PRIMARY KEY NOT NULL,
            start INTEGER NOT NULL,
            end INTEGER NOT NULL
            )"""
    )

    con.execute(
        """
        CREATE TABLE Checksum(
            checksumId INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
            partId INTEGER NOT NULL,
            hash)"""
    )

    con.execute(
        """
        CREATE TABLE OutputFile(
            OutputFileId INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
            partId INTEGER NOT NULL,
            pathStr TEXT NOT NULL)"""
    )

    _populate_connection(con, path_to_target.stat().st_size, workDirectoryInfo)

    return con
