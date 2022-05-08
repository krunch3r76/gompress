import platform
from multiprocessing import Process
import subprocess
import time
if platform.system() == "Windows":
    import winsound
ssp = None
def play_sound(path_to_sound_file, sleeptime=0):
    print(path_to_sound_file)
    if platform.system() == "Windows":
        ssp = Process(
            target=winsound.PlaySound,
            args=(str(path_to_sound_file), winsound.SND_FILENAME),
            daemon=False,
        )
        ssp.start()
        time.sleep(sleeptime)
    elif platform.system() == "Linux":
        ssp = subprocess.Popen(
            ["aplay", str(path_to_sound_file)],
            # ["aplay", "gs/transformers.wav"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    elif platform.system() == "Darwin":
        ssp = subprocess.Popen(
            ["afplay", str(path_to_sound_file)],
            # ["afplay", "gs/transformers.wav"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
