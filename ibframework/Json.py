from pathlib import Path
import logging
import sys

import pandas as pd

import utils

from Config import json_config
import json
from python_json_config import ConfigBuilder
from datetime import datetime

import ib_insync

# parse config
config = json_config()

# config logging
loggingPath = config.params.data.logs_output_dir + config.params.data.log_filename
logging.basicConfig(filename=loggingPath, level=logging.CRITICAL)
log = logging.getLogger('broadcast')

class JsonFiles:
    def __init__(self):
        self.jsonDir = Path(config.params.data.json_output_dir)
        if not self.jsonDir.exists():
            self.jsonDir.mkdir()

    def readFile(self, type, name):
        if type == "contract":
            contract = utils.createBtContract(name)
            filepath = Path(self.jsonDir.joinpath(contract + ".json"))
            #print(filepath, filepath.exists())
        else:
            filepath = Path(self.jsonDir.joinpath(name + ".json"))
            #print(filepath, filepath.exists())
        if filepath.exists():
            with open(filepath, 'r') as file:
                json_data = dict(json.load(file))
                file.close()
            return json_data, filepath
        else:
            return False, filepath

    def readValue(self, type, name, value):

        json_data, filepath = self.readFile(type, name)
        if json_data is not False:
            return json_data[value] if value in json_data else False, filepath
        else:
            return False, filepath

    def updateFile(self, type, name, value):

        json_data, filepath = self.readFile(type, name)
        if json_data is not False:
            #print(json_data)
            with open(filepath, 'w') as file:
                json_data.update(value)
                json.dump(json_data, file)
                file.close()
                #print('file created')
                #print(self.readFile(type, name))
        else:
            with open(filepath, 'x') as file:
                json.dump(value, file)
                file.close()
                #print('file created')
                #print(self.readFile(type, name))


