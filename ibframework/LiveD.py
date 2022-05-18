from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import logging
import os
import json
import argparse

import backtrader as bt
from btplotting import BacktraderPlottingLive
from btplotting.schemes import Blackly
from btplotting.analyzers import RecorderAnalyzer
from datetime import time, datetime, timedelta
import pytz
import asyncio
from aiogram import Bot, Dispatcher, types
from aiogram.utils import exceptions, executor
import ib_insync

import Indicators
from Table_display import table_display
from Cerebro import TFRAME, COMP
from Acquire import SUFIX
import utils
from Strategies import Macd_AtrTrail_M_Dual as LiveBaseStrategy

from Json import JsonFiles
from Config import json_config

# config logging
logging.basicConfig(level=logging.CRITICAL)
log = logging.getLogger('broadcast')

# parse config
config = json_config()
# parse Json
jsonfile = JsonFiles()

_logger = logging.getLogger(__name__)

# ---------------------------Telegram-----------------------------------

API_TOKEN = "1488106539:AAEyd94XhEL9HqZiCjexok-jS6IUFpjzPWg"
USER_ID = 470142411


bot = Bot(token=API_TOKEN, parse_mode=types.ParseMode.HTML)
dp = Dispatcher(bot)


async def send_message(user_id: int, text: str, disable_notification: bool = False) -> bool:
    """
    Safe messages sender

    :param user_id:
    :param text:
    :param disable_notification:
    :return:
    """
    try:
        await bot.send_message(user_id, text, disable_notification=disable_notification)
    except exceptions.BotBlocked:
        log.error(f"Target [ID:{user_id}]: blocked by user")
    except exceptions.ChatNotFound:
        log.error(f"Target [ID:{user_id}]: invalid user ID")
    except exceptions.RetryAfter as e:
        log.error(f"Target [ID:{user_id}]: Flood limit is exceeded. Sleep {e.timeout} seconds.")
        await asyncio.sleep(e.timeout)
        return await send_message(user_id, text)  # Recursive call
    except exceptions.UserDeactivated:
        log.error(f"Target [ID:{user_id}]: user is deactivated")
    except exceptions.TelegramAPIError:
        log.exception(f"Target [ID:{user_id}]: failed")
    else:
        log.info(f"Target [ID:{user_id}]: success")
        return True
    return False


async def broadcaster(message) -> int:
    """
    Simple broadcaster

    :return: Count of messages
    """
    count = 0
    try:
        if await send_message(USER_ID, message):
            count += 1
        await asyncio.sleep(.05)  # 20 messages per second (Limit: 30 messages per second)
    finally:
        log.info(f"{count} messages successful sent.")

    return count


# ---------------------------Telegram-----------------------------------

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

def _run_live(strategy) -> bt.Strategy:

    _logger.info("Constructing Cerebro")
    # Load config parameters
    stratFunc = eval(config.get_strategy_parameter(strategy, "base_strategy"))
    stratParameters = config.config_data["strategies"][strategy]["live"]
    contractNames = config.get_contract_names_by_strat(strategy)
    dualData = config.get_strategy_parameter(strategy, "dual_data")
    percentSizer = config.get_strategy_parameter(strategy, "percent_sizer")
    candleSize = config.get_strategy_parameter(strategy, "candle_size")
    timeFrame = TFRAME[candleSize]
    compression = COMP[candleSize]
    longCandleSize = config.get_strategy_parameter(strategy, "long_candle_size")
    longTimeFrame = TFRAME[longCandleSize]
    longCompression = COMP[longCandleSize]
    gatewayPort = config.params.connection.gateway_port
    gatewayIp = config.params.connection.gateway_ip
    debug = config.params.main.debug
    clientId = config.get_strategy_parameter(strategy, "client_Id")
    print("clientId", clientId)
    timeout = config.params.connection.ib_insync_timeout
    timezone = pytz.timezone(config.params.main.timezone)

    # Initialize Cerebro
    cerebro = bt.Cerebro()
    # Initialize Store
    store = bt.stores.IBStore(host=gatewayIp, port=gatewayPort, _debug=debug, clientId=clientId, notifyall=False,
                              reconnect=-1, timeout=timeout)
    # Initialize Broker
    cerebro.broker = store.getbroker()
    # Set Sizer
    cerebro.addsizer(Indicators.finalSizer, percentSizer=percentSizer)
    # Set Strategy
    cerebro.addstrategy(stratFunc, strat=strategy, **stratParameters)
    # Load analyzer switches
    analyzers = config.config_data["analyzers"]
    # Set analyzers
    if analyzers["live_plot"]:
        cerebro.addanalyzer(RecorderAnalyzer)
        cerebro.addanalyzer(BacktraderPlottingLive, volume=False, scheme=Blackly(
                            hovertool_timeformat='%F %R:%S'), lookback=120)

    for cont in contractNames:

        name = cont + SUFIX[candleSize]
        long_name = cont + SUFIX[longCandleSize]

        d = createBtContract(cont)

        print("dataname", d)

        data0 = store.getdata(dataname=d, rtbar=True, qcheck=0.1, timeframe=bt.TimeFrame.Ticks, compression=30)
        cerebro.resampledata(data0, timeframe=timeFrame, compression=compression, name=name)

        if dualData:
            cerebro.resampledata(data0, timeframe=longTimeFrame, compression=longCompression, name=long_name)

    #cerebro.add_rttimer(when="20:04", repeat=1, cerebro=cerebro, tz=timezone)
    #cerebro.add_timer(when=time(19, 31), repeat=timedelta(minutes=1), cheat=False, tzdata=timezone, realtime=True)

    res = cerebro.run(tz=timezone)
    return cerebro, res[0]

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(name)s:%(levelname)s:%(message)s', level=logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument("strategy")
    args = parser.parse_args()
    print(args.strategy)
    cerebro, strat = _run_live(args.strategy)