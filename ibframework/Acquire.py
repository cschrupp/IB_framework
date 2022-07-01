import logging
import sys
from pathlib import Path

import pandas as pd

import utils

from Config import json_config
import json
from python_json_config import ConfigBuilder
from datetime import datetime

import ib_insync

# parse config
config = json_config()

# Create Logging dir
logsDir = Path(config.params.data.logs_output_dir)
if not logsDir.exists():
    logsDir.mkdir()
# config logging
loggingPath = config.params.data.logs_output_dir + config.params.data.log_filename
logging.basicConfig(filename=loggingPath, level=logging.CRITICAL)
log = logging.getLogger('broadcast')


# ----------------------ACQUIRE CONTRACT DATA------------------------ # START

SUFIX = {'1 month': '-1MO', '1 week': '-1W', '1 day': '-1D', '8 hours': '-8H', '4 hours': '-4H',
         '3 hours': '-3H', "2 hours": "-2H", '1 hour': '-1H', '30 mins': '-30M', '20 mins': '-20M', '15 mins': '-15M',
         '10 mins': '-10M', '5 mins': '-5M', '3 mins': '-3M', '2 mins': '-2M', '1 min': '-1M'}
MAX = {"1 month": "5 Y", "1 week" : "5 Y", "1 day" : "1 Y", '8 hours': '34 D', '4 hours': '34 D',
       '3 hours': '34 D', "2 hours": "34 D", '1 hour': '34 D', '30 mins': '34 D', '20 mins': '27 D', '15 mins': '20 D',
       '10 mins': '13 D', '5 mins': '10 D', '3 mins': '10 D', '2 mins': '10 D', '1 min': '10 D'}


class acquireData():

    def __init__(self):

        # Config parameters
        dateFormat = "%Y-%m-%d"
        #self.current = datetime.strptime(config.params.main.end_date, dateFormat)
        self.duration = config.params.data.duration
        self.gateway_ip = config.params.connection.gateway_ip
        self.gateway_port = config.params.connection.gateway_port
        self.ibapiClientId = config.params.connection.ibapi_client_id
        self.ib_insycClientId = config.params.connection.ib_insync_client_id
        self.ib_insycTimeout = config.params.connection.ib_insync_timeout
        self.dataOutput = config.params.data.dataOutput
        self.offline = config.params.main.offline
        self.dataRetries = config.params.data.data_retries
        self.dataType = config.params.data.data_type
        self.useRTH = config.params.data.use_rth
        self.contractNames = config.get_all_contract_names()
        self.updMargin = config.params.data.update_margin

        # Create dataframes
        self.contracts = list()
        self.dataFrame = dict()

        # Start Execution
        utils.check_path(self.dataOutput)

        self.create_contracts()

        self.ib = ib_insync.IB()

        for contract in self.contracts:

            candle_size = config.get_contract_parameter(contract.symbol, "candle_size")
            long_candle_size = config.get_contract_parameter(contract.symbol, "long_candle_size")
            dualData = config.get_contract_parameter(contract.symbol, "dual_data")
            name = contract.symbol + SUFIX[candle_size]
            long_name = contract.symbol + SUFIX[long_candle_size]
            rth = "-RTH"
            if self.useRTH:
                filename = self.dataOutput + name + rth + '.csv'
                long_filename = self.dataOutput + long_name + rth + '.csv'
            else:
                filename = self.dataOutput + name + '.csv'
                long_filename = self.dataOutput + long_name + '.csv'

            self.dataFrame[contract.symbol] = dict()

            if self.offline:

                if utils.checkfile(filename):
                    df = pd.read_csv(filename, index_col='date',engine='c', parse_dates=True, low_memory=True)
                    df.index = pd.to_datetime(df.index)
                    self.dataFrame[contract.symbol][name] = df
                else:
                    sys.exit("Candles file NOT found...set main configuration to offline = "
                             "false to retrieve candles from IB")

                if dualData and utils.checkfile(long_filename):
                    df = pd.read_csv(long_filename, index_col='date',engine='c', parse_dates=True, low_memory=True)
                    df.index = pd.to_datetime(df.index)
                    self.dataFrame[contract.symbol][long_name] = df
                    if not utils.checkfile(long_filename):
                        sys.exit("Candles file NOT found...set main configuration to offline = "
                                 "false to retrieve candles from IB")

            else:

                self.ib.connect(self.gateway_ip, self.gateway_port, clientId=self.ib_insycClientId,
                                timeout=self.ib_insycTimeout)
                self.ib.qualifyContracts(contract)
                update = True if utils.checkfile(filename) else False
                df = self.getCandles(contract, candle_size, filename, update)
                self.dataFrame[contract.symbol][name] = df

                if dualData:

                    update = True if utils.checkfile(long_filename) else False
                    df = self.getCandles(contract, long_candle_size, long_filename, update)
                    self.dataFrame[contract.symbol][long_name] = df

                #if self.updMargin:
                #    self.updateMargin(contract)

                self.ib.disconnect()

    def create_contracts(self):

        for name in self.contractNames:

            contract = ib_insync.ib.Contract()
            contract.symbol = name
            contract.localSymbol = name
            contract.secType = config.get_contract_parameter(name, "sectype")
            contract.secType = "CONTFUT" if contract.secType == "FUT" else contract.secType
            contract.currency = config.get_contract_parameter(name, "currency")
            contract.exchange = config.get_contract_parameter(name, "exchange")
            contract.lastTradeDateOrContractMonth = config.get_contract_parameter(name, "last_trade") if \
                                                    contract.secType == "FUT" else ""

            self.contracts.append(contract)

            logging.info("Contract created", name)

    def get_dataframe(self):

        return self.dataFrame

    def getCandles(self, contract, candle_size, filename, update):

        maxDays = MAX[candle_size]
        #timeout = self.ib_insycTimeout

        if update:
            existingDf = pd.read_csv(filename, index_col='date',engine='c', parse_dates=True, low_memory=True)

            earliestDate = existingDf.index[len(existingDf) - 1]
            print(f'{contract.symbol}: Updating data set')
            print("Trying to retrieve candles back to last locally stored date/time:", earliestDate)
        else:
            earliestDate = self.ib.reqHeadTimeStamp(contract, whatToShow=self.dataType, useRTH=self.useRTH)
            print(f'{contract.symbol}: Requesting full data set')
            print("Trying to retrieve candles back to earliest available date/time:", earliestDate)

        firstCandleDate = ''
        barsList = []
        complete = False
        dataRetries = self.dataRetries

        while True:
            bars = self.reqHistData(contract, firstCandleDate, maxDays, candle_size)

            if complete:
                break

            elif not dataRetries:
                break

            elif not bars:
                print(f"Timeout:{dataRetries} Restarting last request")
                dataRetries -= 1
                """
                if dataRetries == 5:
                    #timeout = int(timeout*4)
                    #print("timeout", timeout)
                    self.ib.disconnect()
                    self.ib.connect(self.gateway_ip, self.gateway_port, clientId=self.ib_insycClientId, timeout=timeout)
                """
                if dataRetries == 5:
                    maxDays = '5 D'
                elif dataRetries == 3:
                    maxDays = '1 D'


                continue

            else:
                dataRetries = self.dataRetries
                barsUtil = ib_insync.util.df(bars)
                barsList.append(bars)
                firstCandleDate = bars[0].date
                last_dt_string = barsUtil.date[0]
                #print("Types, last_dt_string", type(last_dt_string), type(pd.Timestamp(last_dt_string)), "earliestDate", type(earliestDate))
                if type(last_dt_string) is datetime.date:
                    complete = last_dt_string <= earliestDate.date()

                else:
                    try:
                        complete = pd.Timestamp(last_dt_string) <= earliestDate
                    except TypeError:
                        complete = last_dt_string <= earliestDate.date()

                print(f'{contract.symbol}: Fetching {candle_size} candles down to {firstCandleDate}')

        allBars = [b for bars in reversed(barsList) for b in bars]
        df = ib_insync.util.df(allBars)
        df.set_index("date", inplace=True)
        df.index = pd.to_datetime(df.index)

        if update:
            # Combine existing candles with new candles
            df = pd.concat([existingDf, df])
            # dataframe cleanup
            df = df.groupby(df.index).first()
            df.dropna(inplace=True)

        # save to CSV file
        df.to_csv(filename)

        return df

    def reqHistData(self,contract, lastcandle, duration, candle_size):

        # print(contract,lastcandle,duration,candle_size)

        bars = self.ib.reqHistoricalData(
                              contract=contract,
                              endDateTime=lastcandle,
                              durationStr=duration,
                              barSizeSetting=candle_size,
                              whatToShow=self.dataType,
                              useRTH=self.useRTH,
                              formatDate=1,
                              keepUpToDate=False,
                              chartOptions=[]
        )
        # Throttle to avoid 'Pacing violation'
        self.ib.sleep(11)
        return bars

    def updateMargin(self,contract):

        start = ''
        end = datetime.now()
        ticks = self.ib.reqHistoricalTicks(contract, start, end, 1, 'MIDPOINT', useRth=self.useRTH)
        price = int(ticks[0].price)
        order = ib_insync.LimitOrder('BUY', 1, price)
        margin = self.ib.whatIfOrder(contract, order)
        logging.info(margin.initMarginChange)
        marginChange = round(float(margin.initMarginChange), 2)
        modJson = config.config_data
        modJson["contracts"][contract.symbol]["margin"] = marginChange
        with open('config.js', 'w') as file:
            json.dump(modJson, file)


        """
        path = "./config.js"
        
        with open(path) as jsonFile:
        self.config_data = json.load(jsonFile)

        # create config parser
        builder = ConfigBuilder()

        # Parse config
        self.params = builder.parse_config(path)

        name = "contracts." + contract.symbol + ".margin"

        print(name)

        self.params.update(name, margin.initMarginChange)

        mar = self.params.to_json(path)
        assert mar == margin.initMarginChange

        """










