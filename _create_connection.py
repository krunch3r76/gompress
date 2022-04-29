import sqlite3
from debug.mylogging import g_logger


def _partition(total, maxcount):
    """return an array of n tuples of start and end lengths measuring length evenly n-1 times"""
    if total == 1:
        return [total]

    if total <= maxcount:
        count = total
    else:
        count = maxcount

    minimum = int(total / count)
    while minimum == 1:
        count -= 1
        minimum = int(total / count)

    extra = int(total % count)

    rv = []
    for _ in range(count - 1):
        rv.append(minimum)
    rv.append(minimum + extra)
    return rv


def _partitionRanges(target_length, part_count):
    lengths = _partition(target_length, part_count)
    ranges = [
        (
            0,
            lengths.pop(0),
        )
    ]
    # lengths.pop(0)
    for i, length in enumerate(lengths):
        next_range = (ranges[i][1], ranges[i][1] + length)
        ranges.append(next_range)
    return ranges


def _populate_connection(con, target_length, workDirectoryInfo, part_count):
    """build meta details and add to database"""
    con.execute(
        """
            INSERT INTO OriginalFile(file_hash, part_count) VALUES (?,?)""",
        (
            workDirectoryInfo.path_to_target_wdir.name,
            part_count,
        ),
    )

    ranges = _partitionRanges(target_length, part_count)
    con.executemany("INSERT INTO Part(start, end) VALUES (?,?)", ranges)


def create_connection(
    path_to_connection_file, path_to_target, workDirectoryInfo, part_count
):
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

    _populate_connection(
        con, path_to_target.stat().st_size, workDirectoryInfo, part_count
    )

    return con
