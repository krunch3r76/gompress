"""implements create_connection to create a database for a compression job specific to a file.

"""
import sqlite3
from debug.mylogging import g_logger

# maxcount being deprecated
def _partition(total, maxsize=None):
    """return an array of end lengths measuring maxsize evenly
    at least n-1 times adding the remainder as the last element.

    Args:
        total: the length to be measured by maxsize
        maxsize: the minimum length or the length that shall measure total

    Example:
        total length of 1001 with maxsize 250 to yield:
        [ 0, 250, 500, 750, 1000, 1001 ]

    
    Called By: _partitionRanges
    """
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
            # rv[-1]+=extra
            rv.append(rv[-1] + extra)
    return rv


def _partitionRanges(target_length, maxsize=None):
    """given a length return an array of ranges [beg, end),... of at least maxsize.

    Args:
        target_length:  the number to be divided
        maxsize:    the minimum (if possible) length of a division

    Returns:
        An array of pairs in representing all full measures of maxsize ending with a partial if applicable

    Example:
        target_length of 1001 with a max size of 250 to yield
        [ (0,250), (250, 500), (500, 750), (750, 1000), (1000, 1) ]


    called by _populate_connection
    """
    # lengths = _partition(target_length, part_count)
    offsets = _partition(target_length, maxsize)
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
    """add rows to tables to describe the work to be done given the size of the original file.

    Pre:
        workDirectoryInfo has initialized the working directory, .e.g by .create_skeleton()

    Args:
        con: connection to database to insert records into
        target_length: size of the file to be compressed
        workDirectoryInfo: object providing information about the working directory specific to this work

    Post:
        records inserted into |OriginalFile| and |Part| to record part count and ranges to be worked on

    Returns: None

    called by: create_connection
    """

    ranges = _partitionRanges(target_length, None)

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
    """create a new database and return the connection.

    Every job to compress shall have a database to keep information about the work
    incuding the ranges of the target file to be compressed independently |Part|,
    the expected checksum or length of the downloaded part to verify the downloaded
    part |Checksum|, the paths to the processed parts before they are concatenated
    |OutputFile|, and the path to the file to be compressed |OriginalFile|.

    Pre:
        None

    Args:
        path_to_connection_file: the Path object to the database to be created
        path_to_targer: the Path to the target that will be compressed
        workDirectoryInfo: the WorkDirectoryInfo object to prepare the working directory
            including to create it before creating the database in it

    Post:
        the working directory for the target has been created and the initial database
        connection added

    Returns:
        sqlite3 connection object in autocommit mode

    Connection details:
    OriginalFile
    ------------
    originalFileId {pk}
    file_hash TEXT
    part_count INT

    Part
    ------------
    partId {pk}
    start INT
    end INT

    Checksum
    ------------
    checksumId {pk}
    partId INTEGER {fk}
    hash TEXT

    OutputFile
    -------------
    OutputFileId {pk}
    partId INTEGER {fk}
    pathStr TEXT


    Example:
        the target file is /tmp/blah.raw. $WORKDIR/<hash of target file> will be created along
        with subdirectories as defined by the WorkDirectoryInfo object.
        $WORKDIR/<conn name> database will have been created with OriginalFile and Part tables
        containing relevant rows.
    """

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
