import Acquire
import utils
from Config import json_config

# parse config
config = json_config()

startDate = config.params.main.start_date
endDate = config.params.main.end_date

test = Acquire.acquireData()
dataFrame = test.get_dataframe()
#print(dataFrame)
for contract in dataFrame.keys():
    #print(dataFrame[contract])
    for candles in dataFrame[contract]:
        print("Candles", candles)
        #print(dataFrame[contract][candles])



#slicedDataframe = utils.setInterval(dataFrame, startDate, endDate)
#print(slicedDataframe)