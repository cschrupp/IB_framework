import os
import logging
import sys

import pandas as pd
from datetime import datetime
from random import randint
import ib_insync
import pytz

from Config import json_config
from Acquire import SUFIX

# config logging
logging.basicConfig(level=logging.CRITICAL)
log = logging.getLogger('broadcast')

# parse config
config = json_config()


def maxPeriodDays(strategy, candles):

    DAYS = {'1 month': 1/20, '1 week': 1/5, '1 day': 1, '8 hours': 1.625, '4 hours': 3.25,
            '3 hours': 4.3, "2 hours": 6.5, '1 hour': 13, '30 mins': 26, '20 mins': 39, '15 mins': 52,
            '10 mins': 78, '5 mins': 156, '3 mins': 260, '2 mins': 390, '1 min': 780}

    period_dict = {tuple(["percentile_period"]): ["percentile_open", "percentile_close"],
                   tuple(["bb_period"]): ["bb_open", "bb_close"],
                   tuple(["macd1", "macd2", "macdsig"]): ["macd_open", "macd_close"],
                   tuple(["atrperiod"]): ["atr_close"],
                   tuple(["smaperiod", "dirperiod"]): ["trend_open", "trend_close"],
                   tuple(["osc_period"]): ["osc_open", "osc_close"]
                   }

    safety_factor = 2

    max_period = list()
    for p in period_dict:
        for t in p:
            enabled = list()
            for e in period_dict[p]:
                enabled.append(config.get_strategy_parameter(strategy, e))
            if any(enabled):
                max_period.append(config.get_strategy_parameter(strategy, t))
    try:
        days = max(max_period) / DAYS[candles]
    except ValueError:
        days = 0
    if days < 1:
        days = 1

    return int(days * safety_factor)


def parseTimes(time):

    try:
        time_format = "%H:%M:%S"
        dt_object = datetime.strptime(time, time_format)
        hours = dt_object.hour
        minutes = dt_object.minute
        seconds = dt_object.second
    except ValueError:
        time_format = "%H:%M"
        dt_object = datetime.strptime(time, time_format)
        hours = dt_object.hour
        minutes = dt_object.minute
        seconds = dt_object.second

    return hours, minutes, seconds

def parseDates(date):
    split_date = date.split("-")
    year = int(split_date[0])
    month = int(split_date[1])
    day = int(split_date[2])
    return year, month, day


def readCSV(contract,candle_size):
    useRTH = config.params.data.use_rth
    dataOutput = config.params.data.dataOutput
    name = contract + SUFIX[candle_size]
    rth = "-RTH"
    if useRTH:
        filename = dataOutput + name + rth + '.csv'
    else:
        filename = dataOutput + name + '.csv'
    df = pd.read_csv(filename, index_col='date',engine='c', parse_dates=True, low_memory=True)
    return df

def checkfile(filename):

    try:
        pd.read_csv(filename)
        check = True
    except FileNotFoundError:
        check = False

    return check

def check_path(pathToCheck):

    # You should change 'test' to your preferred folder.
    check_folder = os.path.isdir(pathToCheck)

    # If folder doesn't exist, then create it.
    if not check_folder:
        os.makedirs(pathToCheck)

# ----------------------SET INTERVAL FOR BACKTESTING------------------------ # START
def setInterval(dataFrame, fromDate, toDate):
    slicedDataframe = dict()
    if fromDate == '' and toDate == '':
        return dataFrame

    for contract in dataFrame.keys():
        slicedDataframe[contract] = dict()
        for candles in dataFrame[contract]:

            if fromDate == '':
                df = dataFrame[contract][candles][(dataFrame[contract][candles] <= toDate)]
            elif toDate == '':
                df = dataFrame[contract][candles][(dataFrame[contract][candles] >= fromDate)]
            else:
                df = dataFrame[contract][candles][(dataFrame[contract][candles].index >= fromDate) &
                                                  (dataFrame[contract][candles].index <= toDate)]
            slicedDataframe[contract][candles] = df

    return slicedDataframe

# ----------------------SET INTERVAL FOR BACKTESTING------------------------ # END

class timer:
    def __init__(self):
        self.starTime = None
        self.stopTime = None
        self.elapsedTime = None

    def setStartTime(self):
        # Start time counter
        self.starTime = datetime.now()

    def setStopTime(self):
        # Stop time counter
        self.stopTime = datetime.now()
        self.elapsedTime = (self.stopTime - self.starTime).total_seconds()

    def printElapsedTime(self):
        print('')
        print(f'This run took {round(self.elapsedTime / 60, 1)} minutes')
        print('')
        print('================================================================================')

def createBtContract(contractName):

    contractList = list()
    contractList.append(contractName)
    contractList.append(config.get_contract_parameter(contractName, "sectype"))
    contractList.append(config.get_contract_parameter(contractName, "exchange"))
    contractList.append(config.get_contract_parameter(contractName, "currency"))
    if config.get_contract_parameter(contractName, "sectype") == "FUT":
        contractList.append(config.get_contract_parameter(contractName, "last_trade"))
        contractList.append(str(config.get_contract_parameter(contractName, "multiplier")))
    contract = "-".join(contractList)

    return contract

def createIbInsycncontract(contractName):

    contract = ib_insync.ib.Contract()
    contract.symbol = contractName
    contract.localSymbol = contractName
    contract.secType = "CONTFUT" if config.get_contract_parameter(contractName, "sectype") == "FUT" else \
                       config.get_contract_parameter(contractName, "sectype")
    contract.currency = config.get_contract_parameter(contractName, "currency")
    contract.exchange = config.get_contract_parameter(contractName, "exchange")
    if config.get_contract_parameter(contractName, "sectype") == "FUT":
        contract.lastTradeDateOrContractMonth = config.get_contract_parameter(contractName, "last_trade")
        contract.multiplier = config.get_contract_parameter(contractName, "multiplier")

    return contract


def ib_connect(ib):
    gatewayPort = config.params.connection.gateway_port
    gatewayIp = config.params.connection.gateway_ip
    # clientId = config.params.connection.ib_insync_client_id
    clientId = randint(1000, 9999)
    timeout = config.params.connection.ib_insync_timeout

    if ib.isConnected():
        return True
    else:
        try:
            ib.connect(gatewayIp, port=gatewayPort, clientId=clientId, timeout=timeout)
        except:
            return False
        if ib.isConnected():
            return True
        else:
            return False


def ib_disconnect(ib):

    if ib.isConnected():
        ib.disconnect()
    else:
        return


def ibPricing(dataname, size=1):

    timezone = pytz.timezone(config.params.main.timezone)

    ib = ib_insync.IB()
    ib_connect(ib)

    print("Dataname", dataname)

    name = str(dataname).split("-")[0]
    contract = createIbInsycncontract(name)

    ib.qualifyContracts(contract)
    result = {}

    ib.reqMktData(contract, '', False, False)
    ib.sleep(2)
    islive = ib.ticker(contract)
    ib.cancelMktData(contract)
    result["islive"] = islive.hasBidAsk()


    ticks = ib.reqHistoricalTicks(
        contract=contract,
        startDateTime="",
        endDateTime=datetime.now(tz=timezone),
        numberOfTicks=1,
        whatToShow="Bid_Ask",
        useRth=False,
        ignoreSize=False,
    )
    result["bid"] = ticks[-1].priceBid
    result["ask"] = ticks[-1].priceAsk

    order = ib_insync.LimitOrder('BUY', size, result["bid"])
    margin = ib.whatIfOrder(contract, order)
    marginChange = round(abs(float(margin.initMarginChange)), 2)
    result["margin"] = marginChange

    ib_disconnect(ib)
    #print("Result", dataname, result)
    return result
