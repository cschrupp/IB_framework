import logging
import subprocess

import utils
import Acquire
import Cerebro
import Results

from Config import json_config


# parse config
config = json_config()

# Create Logging dir
logsDir = config.params.data.logs_output_dir
logFilename = config.params.data.log_filename
fullLogsFilename = logsDir + "/" + logFilename
utils.check_path(logsDir)

# config logging
logging.basicConfig(level=logging.CRITICAL, filename=fullLogsFilename)
log = logging.getLogger('broadcast')

import sys
import os
import subprocess
import time
from pathlib import Path
import signal

# Config parameters
mode = config.params.main.mode
strategyList = config.get_all_strategy_names()
logging.debug("strategy list", strategyList)
cmd = Path('C:/Windows/system32/WindowsPowerShell/v1.0/powershell.exe')
path = Path("C:/B21/")
print(cmd)
lives = dict()
for strat in strategyList:
    #lives[strat] = subprocess.Popen([sys.executable, "C:/B21/LiveD.py", strat],stdin=subprocess.PIPE,
    #stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=1000,text=True)#, shell=True)
    lives[strat] = subprocess.Popen(['start', "python", "LiveD.py", strat], shell=True)
    print("Pid: ", lives[strat].pid)
    #print(os.getpgid(lives[strat].pid))
    time.sleep(5)
"""
while True:
    for strat in strategyList:
        print("Communicating with: ", lives[strat])
        #output = subprocess.check_output([sys.executable, "C:/B21/LiveD.py", strat], stderr=subprocess.STDOUT, timeout=10)
        #output, errors = lives[strat].communicate()
        lines = lives[strat].stdout.read()
        #lives[strat].wait()
        print(lines)
        #print(output)
        #print(errors)

print("open ended")

time.sleep(5)
print("waited 5")
for strat in strategyList:
    print("kill started")
    lives[strat].kill()
    #os.kill(lives[strat].pid, signal.CTRL_BREAK_EVENT)
    #subprocess.Popen("TASKKILL /F /PID {pid} /T".format(pid=lives[strat].pid))
    lives[strat].wait(timeout=5)
    #os.kill(lives[strat].pid, signal.SIGINT)
    #os.kill(os.getpgid(lives[strat].pid), signal.SIGTERM)
    time.sleep(5)
"""