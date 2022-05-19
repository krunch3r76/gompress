"""provides an archive function to tar an input list of a directory or several files

requirements:
    shall utilize shared cross platform features only
    shall enable gompress to archive files with input as /some/path/*
        on unix, this will be a list via globbing at shell level
        on windows, the wildcard shall have the list reinterpreted to glob (applied to each)
    the inputs shall be sorted before tar'ed
    the list passed to tar shall contain unique elements only
    for multiple files, a common root is identified and the list is reinterpreted as relative to that root

issues:
    undefined result when interrupted and resumed if target files are modified in interim
"""

# authored by krunch3r (https://www.github.com/krunch3r76)
# license GPL 3.0

from pathlib import Path
from subprocess import run
from tempfile import TemporaryDirectory
import sys
from debug.mylogging import g_logger


def _find_common_root(paths):
    """identifies and returns the highest shared root path and its zero-based depth

    starts with depth 0 (root) and examines next depth if all paths share root path
    in common with the next root at the next depth.

    Args:
        paths:  a sequence of Path castable objects (e.g. path strings or Paths)

    Process:
        [ >1 path ]
        loop:
            list at depth
            [ list uniform ]
            increment depth
        concatenate parents

        [ 1 path ]
        concatenate parents

    Returns:
        Path object of common root
        depth of common root

    Raises: None
    """
    from functools import reduce

    paths = [Path(path) for path in paths]  # vestigial to list+rewrap into Path
    pathToSharedRoot = None
    depth = 0

    if len(paths) > 1:
        whetherLevelIsShared = True
        while whetherLevelIsShared:
            partsListAtLevel = [path.parts[depth] for path in paths]
            reduced1 = reduce(lambda a, b: a if a == b else None, partsListAtLevel)
            if reduced1 is None:
                whetherLevelIsShared = False
            else:
                whetherLevelIsShared = True
                depth += 1
        shared_depth = depth - 1
        root = Path(paths[0].parts[0])
        pathToSharedRoot = root.joinpath(*paths[0].parts[1 : shared_depth + 1])
    else:
        pathToSharedRoot = paths[0].parents[0]
        shared_depth = len(paths[0].parts)
    return pathToSharedRoot, shared_depth


def _strip_root_from_paths(paths, pathToCommonRoots):
    """maps list unto itself sans paths.

    Args:
        paths:  stringable sequence of paths (e.g. Path objects)
        pathToCommonRoots:  stringable path of the parents common to all of \paths\

    Process:
        map relative common
        normalize from Path #review

    Returns:
        the paths input with the common root remapped to "."

    Raises:
        None
    """

    stripped_paths_str = []
    pathToCommonRootsStr = str(pathToCommonRoots)
    for path in paths:
        path_stripped = str(path).replace(str(pathToCommonRoots), ".")
        stripped_paths_str.append(path_stripped)

    return [str(Path(stripped_path_str)) for stripped_path_str in stripped_paths_str]


class UserTempPath:
    """creates a temporary directory and prepares a pure Path to a child.

    allows for a temporary directory to persist along with its associated child
    """

    def __init__(self, file):
        self.tempDir = TemporaryDirectory(prefix="gompress_")
        self.data = Path(self.tempDir.name) / file


def _establish_temporary_tar(files: list, target_basename):
    """formats a name for the target tar file and returns a temporary directory for it.

    affixes if not present the .tar extension to a basename, which if not provided
    uses the stem of the first file in the list + '_gompressed'.

    Args:
        files: Path-castable list of files
        target_basename: optional name of tar file with or without tar extension

    Process:
        [ no preference ]
        pick first stem
        [ >1 files ]
        affix '_gompressed.tar'

        [ 1 file ]
        affix '.tar'


        [ preference ]
        ensure '.tar' ending

    Returns:
        A UserTempPath object holding an abstract path to the target tar

    Raises:
        None
    """
    # pick a basename for the tar file
    if target_basename is None:
        if len(files) > 1:
            target_path_str = Path(files[0]).stem + "_gompressed.tar"
        else: # implies directory
            target_path_str = Path(files[0]).stem + ".tar"
        # TODO, check for duplicate name

        # if not target_path_str.endswith(".tar"):
        #     target_path_str += ".tar"
        target_path = Path(target_path_str)
    else:
        if not target_basename.endswith(".tar"):
            target_basename += ".tar"
        target_path = Path(target_basename)
    tarFile = UserTempPath(target_path)
    return tarFile


def _normalize_input_files(files: list):
    """remap files list input to glob raw * characters


    on windows, python will not glob when invoked from powershell, manually done here

    Args:
        files: Path castable list of files

    Process:
        on each file
            [ '*' in name ]
            expand
            append

            [ '*' in name ]
            append

    Returns:
        a list in which all starred expression have been expanded to their globbed
        directory listings

    Raises:
        none
    """

    remapped_list = []
    for file in files:
        if "*" in file:
            globexp = Path(file).name
            baseDirPath = Path(file).parents[0]
            globbed = list(baseDirPath.glob(globexp))
            remapped_list.extend(globbed)
        else:
            remapped_list.append(file)

    # if len(files) == 1 and "*" in files[0]:
    #     globexp = Path(files[0]).name
    #     baseDirPath = Path(files[0]).parents[0]
    #     files = list(baseDirPath.glob(globexp))
    return remapped_list


def archive(files, target_basename=None):
    """archives files or single directory into a temporary tar file and returns

    given a directory or list of files or glob expression (windows), create a tar file of
    the (expanded) list referenced in a self deleting user Path object UserTempPath.
    the tar file is named based on the input stem or derived from the first file in the list.

    if //target_basename// is not provided, the archive is named after the name of the first file
    e.g.
    files = ['/tmp/totar/17635.txt', '/tmp/totar/17677.txt', '/tmp/totar/17667.txt']
    will write a temporary file of the name 17635_gompressed.tar to a temporary directory and return
    the path to the temporary file

    Args:
        files: a Path castable sequence of file(s) or on windows glob expressions

    Process:
        normalize input
        make unique
        sort
        find common root
        strip common root
        establish temporary tar

    Returns:
        a UserTempPath object that references the created tar file in a temporary directory

    Raises:
        None
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
    return tarFileTarget


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Process some files.")
    parser.add_argument("files", nargs="+")
    args = parser.parse_args()
    print(f"file arguments on cli: {args.files}")
    print()
    archive(args.files)
