from pathlib import Path
import hashlib

def sha1_hash(path_to_target):
    sha1 = hashlib.sha1()
    with open(path_to_target, 'rb') as f:
        while True:
            data = f.read(4096)
            if not data:
                break
            sha1.update(data)
    the_hash = sha1.hexdigest()
    return the_hash

class WorkDirectoryInfo:
    """
        / path_to_wdir_parent           .../
        / path_to_target_wdir           ..././<hash>/
        / path_to_parts_directory       ..././<hash>/parts
        / path_to_final_directory       ..././<hash>/final
    """
    def __init__(self, path_to_wdir_parent_in, path_to_target_in):
        """ hash path_to_target to create path_to_wdir under parent """
        self.__path_to_wdir_parent = path_to_wdir_parent_in
        self._path_to_target = path_to_target_in
        # hash path_to_target
        the_hash = sha1_hash(self._path_to_target)
        # create abstract path to wdir from hash
        self.__path_to_target_wdir = self.path_to_wdir_parent / the_hash
        # create abstraction of parts directory
        self.__path_to_parts_directory = self.path_to_target_wdir / "parts"
        # create abstraction of final directory
        self.__path_to_final_directory = self.path_to_target_wdir / "final"

    @property
    def path_to_wdir_parent(self):
        return self.__path_to_wdir_parent

    @property
    def path_to_target_wdir(self): # contains db file
        return self.__path_to_target_wdir

    @property
    def path_to_parts_directory(self):
        return self.__path_to_parts_directory

    @property
    def path_to_final_directory(self):
        return self.__path_to_final_directory

    def create_skeleton(self):
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
