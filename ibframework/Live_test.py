import os
import time
from Json import JsonFiles
import argparse

jsonfile = JsonFiles()


def loop(strategy):
    repeat = 5
    while repeat:
        wpid = os.getpid()
        jsonfile.updateFile("strategy", "Test PID", {"PID": wpid})
        print("Strategy", strategy, "PID file updated. PID=", wpid, "Repeat=", repeat)
        time.sleep(10)
        repeat -= 1

    value, path = jsonfile.readFile("strategy", "Test PID")
    print("Value=", value, "Path=", path)

    if value:
        os.remove(path)
        print("PID file deleted")

    else:
        print("File does not exist")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("strategy")
    args = parser.parse_args()
    print(args.strategy)
    loop(args.strategy)
