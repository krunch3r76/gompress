"""create the skeleton for a working subdirectory pertinent to a file to compress
then provide details as requested.

gompress creates a working subdirectory in a specified workdir named after the hash
of the file to compress. inside the subdirectory, parts and final subdirectories
are created.

also provides a checksum method for general use that returns a length or sha1 hash
of a file


Typical usage example:

workDirectoryInfo = WorkDirectoryInfo("/root-work-dir", "/target")
workDirectoryInfo.create_skeleton()
"""

from pathlib import Path
import hashlib
from debug.mylogging import g_logger

def sha1_hash(path_to_target):
    """perform a sha1 hash on a target file and return."""
    sha1 = hashlib.sha1()
    with open(path_to_target, "rb") as f:
        while True:
            data = f.read(4096)
            if not data:
                break
            sha1.update(data)
    the_hash = sha1.hexdigest()
    return the_hash


def checksum(path_to_target, sha1=False):
    """perform a checksum on a target file to return the length or sha1 hash of a file.

    Args:
        path_to_target: the Path to the file to hash
        sha1: whether to use sha1 algorithm or just return the length of the file

    Post: none

    Returns:
        a string of the checksum
    """
    if not isinstance(path_to_target, Path):
        path_to_target = Path(path_to_target)

    if not sha1:
        return str(path_to_target.stat().st_size)
    else:
        return sha1_hash(path_to_target)


class WorkDirectoryInfo:
    """create the skeleton for a working subdirectory pertinent to a file to compress
       then provide details as requested.

    Constructed with the target and the root workdir and makes changes when the method
    .create_skeleton has been invoked (externally).

    The end result would look like:
        <workdir>/<hash>
        <workdir>/<hash>/parts
        <workdir>/<hash>/final

    Attributes:
        path_to_wdir_parent: Path to the parent or root working directory (for all targets) <workdir>
        path_to_target_wdir: Path to the workdir specific for the target <hash>
        path_to_parts_directory: Path to the parts subdirectory of workdir
        path_to_final_directory: Path to the final subdirectory of workdir
    """

    def __init__(self, path_to_wdir_parent_in, path_to_target_in):
        """add directory information for compression work on a target without creating the directories.

        Args:
            path_to_wdir_parent_in:
                Path to the main working directory under which work directories
                for specific jobs are created (like this one)
            path_to_target_in:
                Path to the file for the job to be run (the file to be compressed)

        Post: None
        """
        self.__path_to_wdir_parent = path_to_wdir_parent_in
        self._path_to_target = path_to_target_in
        # hash path_to_target
        the_hash = checksum(self._path_to_target, sha1=True)
        # create abstract path to wdir from hash
        wdirname = the_hash
        self.__path_to_target_wdir = self.path_to_wdir_parent / the_hash
        # create abstraction of parts directory
        self.__path_to_parts_directory = self.path_to_target_wdir / "parts"
        # create abstraction of final directory
        self.__path_to_final_directory = self.path_to_target_wdir / "final"

    @property
    def path_to_wdir_parent(self):
        return self.__path_to_wdir_parent

    @property
    def path_to_target_wdir(self):  # contains db file
        return self.__path_to_target_wdir

    @property
    def path_to_parts_directory(self):
        return self.__path_to_parts_directory

    @property
    def path_to_final_directory(self):
        return self.__path_to_final_directory

    def create_skeleton(self):
        """initialize by creating empty subdirectories pertinent to the work.

        Post: main working directory and all subdirectories have been created if not already existing 
        """
        self.path_to_wdir_parent.mkdir(exist_ok=True)
        self.path_to_target_wdir.mkdir(exist_ok=True)
        self.path_to_parts_directory.mkdir(exist_ok=True)
        self.path_to_final_directory.mkdir(exist_ok=True)

    def __repr__(self):
        repr_str = f"""
{self.path_to_wdir_parent}
    {self.path_to_target_wdir}
        {self.path_to_parts_directory}
        {self.path_to_final_directory}
"""
        return repr_str
