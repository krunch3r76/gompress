import platform
from multiprocessing import Process
import subprocess
import time

if platform.system() == "Windows":
    import winsound

ssp = None


def play_sound(path_to_sound_file, sleeptime=0):
    global ssp
    if platform.system() == "Windows":
        ssp = Process(
            target=winsound.PlaySound,
            args=(str(path_to_sound_file), winsound.SND_FILENAME),
            daemon=False,
        )
        ssp.start()
        time.sleep(sleeptime)
    elif platform.system() == "Linux":

        if ssp != None:
            try:
                ssp.terminate()
            except:
                pass
            finally:
                ssp = None
        ssp = subprocess.Popen(
            ["aplay", str(path_to_sound_file)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    elif platform.system() == "Darwin":
        if ssp != None:
            try:
                ssp.terminate()
            except:
                pass
            finally:
                ssp = None
        ssp = subprocess.Popen(
            ["afplay", str(path_to_sound_file)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    return ssp
