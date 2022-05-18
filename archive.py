"""provides an archive function to tar an input list


requirements:
    gompress shall emulate tar cross platform features
    gompress shall be able to archive files at /some/path/*
        on unix, this will be a list via globbing at shell level
        on windows, the wildcard shall have the list reinterpreted to glob (applied to each)
    the inputs shall be sorted before tar'ed
    the list is placed in a set to remove duplicate relative paths
    a common root is identified and the list is reinterpreted as relative to that root
    the list is passed to tar

"""

from pathlib import Path
from subprocess import run
from tempfile import TemporaryDirectory
import sys
from debug.mylogging import g_logger


def _find_common_root(paths):
    """looks at each level of the parts and returns the highest common shared level"""
    from functools import reduce

    paths = [Path(path) for path in paths]  # vestigial to list+rewrap into Path
    pathToSharedRoot = None
    level = 0

    #    if len(paths) == 1:
    #        pathToSharedRoot = paths[0].
    if len(paths) > 1:
        whetherLevelIsShared = True
        while whetherLevelIsShared:
            partsListAtLevel = [path.parts[level] for path in paths]
            reduced1 = reduce(lambda a, b: a if a == b else None, partsListAtLevel)
            if reduced1 is None:
                whetherLevelIsShared = False
            else:
                whetherLevelIsShared = True
                level += 1
        shared_level = level - 1
        root = Path(paths[0].parts[0])
        pathToSharedRoot = root.joinpath(*paths[0].parts[1 : shared_level + 1])
    else:
        pathToSharedRoot = paths[0].parents[0]
        shared_level = len(paths[0].parts)
    return pathToSharedRoot, shared_level


def _strip_root_from_paths(paths, pathToCommonRoots):
    stripped_paths_str = []
    pathToCommonRootsStr = str(pathToCommonRoots)
    for path in paths:
        path_stripped = str(path).replace(str(pathToCommonRoots), ".")
        stripped_paths_str.append(path_stripped)
    return [str(Path(stripped_path_str)) for stripped_path_str in stripped_paths_str]


class SelfDestructPath:
    """Creates temporary directory and constructs&wraps a Path object which is deleted upon garbage collection."""

    def __init__(self, file):
        self.tempDir = TemporaryDirectory(prefix="gompress_")
        self.data = Path(self.tempDir.name) / file


#    def __del__(self):
#        try:
#            self.data.unlink()
#            # self.tempDir.cleanup()
#        except:
#            pass


def _establish_temporary_tar(files: list, target_basename):
    # pick a basename for the tar file
    if target_basename is None:
        target_path_str = Path(files[0]).stem
        if not target_path_str.endswith(".tar"):
            target_path_str += ".tar"
        target_path = Path(target_path_str)
    else:
        if not target_basename.endswith(".tar"):
            target_basename += ".tar"
        target_path = Path(target_basename)
    tarFile = SelfDestructPath(target_path)
    return tarFile


def _normalize_input_files(files: list):
    # on windows, python will not glob when invoked from powershell, manually done here
    if len(files) == 1 and "*" in files[0]:
        globexp = Path(files[0]).name
        baseDirPath = Path(files[0]).parents[0]
        files = list(baseDirPath.glob(globexp))
    return files


def archive(files, target_basename=None):
    """archives files into a temporary tar file and returns as a self deleting user Path object.


    if target_basename is not provided, the archive is named after the name of the first file

    e.g.
    files = ['/tmp/totar/17635.txt', '/tmp/totar/17677.txt', '/tmp/totar/17667.txt']
    will write a temporary file of the name 17635.tar to a temporary directory and return
    the path to the temporary file


    process:
        normalize input
        make paths
        find root
        strip root
        establish temporary tar

    """

    files = _normalize_input_files(files)
    paths = {
        Path(file).resolve() for file in files
    }  # string form consistently hashable
    paths = list(paths)  # removes any duplicates
    paths.sort()
    g_logger.debug(f"paths input: {paths}")
    pathToCommonRoot, level_end = _find_common_root(paths)
    g_logger.debug(f"path to common root: {pathToCommonRoot}")
    paths = _strip_root_from_paths(paths, pathToCommonRoot)
    g_logger.debug(f"paths stripped of root: {paths}")
    tarFileTarget = _establish_temporary_tar(files, target_basename)
    run(
        [
            "tar",
            "-c",
            "-C",
            f"{str(pathToCommonRoot)}",
            "-f",
            str(tarFileTarget.data),
            *paths,
        ]
    )
    g_logger.debug(f"target archive file: {tarFileTarget.data}")
    # return SelfDestructPath(str(pathToTargetArchive))
    return tarFileTarget


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Process some files.")
    parser.add_argument("files", nargs="+")
    args = parser.parse_args()
    print(f"file arguments on cli: {args.files}")
    print()
    archive(args.files)
