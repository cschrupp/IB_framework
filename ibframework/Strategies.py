import logging
import os
import subprocess
import time as tm

import pandas as pd

import json
import argparse

import backtrader as bt
from datetime import time, datetime, timedelta
from aiogram.utils import exceptions, executor
import ib_insync
import sqlite3
from sqlite3 import Error
from math import isnan
import pytz
import ib_insync as ib

import Indicators as indicators
from Table_display import table_display
import utils

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

# ----------------------STRATEGIES------------------------ # START


class MultiData:
    def __init__(self):
        pass


class AtrTrail:
    params = (
        ("atr_close", None),
        ("atrperiod", None),
        ("atrdist", None),
        ("trail_on_buy", None),
        ("atrdist_trail", None)
    )

    def __init__(self):
        self.atr
        pass

    def get_indicators(self):
        # Calculate ATR indicator
        atr_in_strategy = self.atr_close
        if atr_in_strategy:
            self.long_inds[d]['atr'] = bt.indicators.AverageTrueRange(d,
                                                                      period=self.atrperiod,
                                                                      subplot=True,
                                                                      plot=True
                                                                      )
        pass

    def set_pstop_value(self):
        pass

    def get_pstop_value(self):
        pass

    def set_long(self):
        pass

    def set_short(self):
        pass

    def set_close(self):
        pass

    def get_long(self):
        pass

    def get_short(self):
        pass

    def get_close(self):
        pass



class Macd_AtrTrail_M_Dual(bt.Strategy):

    params = (
        ("strat", None),
        ("base_strategy", None),
        ("client_Id", None),
        ("start_date", None),
        ("end_date", None),
        ("dual_data", None),
        ("candle_size", None),
        ("long_candle_size", None),
        ("cash", None),
        ("buy_enabled", None),
        ("sell_enabled", None),
        ("close_enabled", None),
        ("invert", None),
        ("gain_close", None),
        ("gain", None),
        ("stoploss_close", None),
        ("stoploss", None),
        ("percentile_open", None),
        ("percentile_close", None),
        ("lowpercentile", None),
        ("highpercentile", None),
        ("percentile_period", None),
        ("bb_open", None),
        ("bb_close", None),
        ("low_bb", None),
        ("high_bb", None),
        ("bb_period", None),
        ("dev_factor", None),
        ("macd_open", None),
        ("macd_close", None),
        ("macd1", None),
        ("macd2", None),
        ("macdsig", None),
        ("atr_close", None),
        ("atrperiod", None),
        ("atrdist", None),
        ("trail_on_buy", None),
        ("atrdist_trail", None),
        ("mean_return_close", None),
        ("trend_open", None),
        ("trend_close", None),
        ("trend", None),
        ("trend_strength", None),
        ("smaperiod", None),
        ("dirperiod", None),
        ("osc_open", None),
        ("osc_close", None),
        ("oscillator", None),
        ("osc_period", None),
        ("osc_ob", None),
        ("osc_os", None),
        ("osc_ob_exit", None),
        ("osc_os_exit", None),
        ("size", None),
        ("percent_sizer", None),
        ("watchdog_market", None)
    )

    def log(self, txt, dt=None):
        ''' Logging function fot this strategy'''
        printLog = config.params.display.enabled
        mode = config.params.main.mode
        if printLog and mode != "optimize":
            dt = dt or self.datas[0].datetime.datetime(0)
            print('%s, %s' % (dt.isoformat(), txt))

    def restart(self):
        print("**********USING RESTART METHOD************")
        strat = self.p.strat
        ibi = ib_insync.IB()
        connected = utils.ib_connect(ibi)
        if connected:
            print("**********USING RESTART METHOD************")
            ibi.disconnect()
            self.cerebro.runstop()
            tm.sleep(5)
            live = subprocess.Popen(['start', "python", "LiveD.py", strat], shell=True)
            print(strat, "Running - Pid: ", live.pid)
            print("**********USING RESTART METHOD************")
        else:
            self.cerebro.runstop()

    def notify_data(self, data, status, *args, **kwargs):
        mode = config.params.main.mode
        if mode == "live":
            print('*' * 5, 'DATA NOTIF:', data._name, data._getstatusname(status), *args)
            if status == data.LIVE:
                self.counttostop = 0
                self.datastatus[data] = 1
                # telegram_msg = '*' * 5 + 'DATA NOTIF: ' + data._name + " " + data._getstatusname(status)
                # executor.start(dp, broadcaster(telegram_msg))
                jsonfile.updateFile("strategy", self.strategy, {data._name: data._getstatusname(status)})
            # Set livestatus to dictionary
                self.livestatus[data] = True
            else:
                self.livestatus[data] = False
            if data._getstatusname(status) == "CONNBROKEN":
                print("data._getstatusname(status)", data._getstatusname(status))
                self.restart()
            elif status == "CONNBROKEN":
                print("status", status)
                self.restart()

    def notify_store(self, msg, *args, **kwargs):
        mode = config.params.main.mode
        if mode == "live":
            print('*' * 5, 'STORE NOTIF:', msg)

            if hasattr(msg, 'errorMsg'):
                if hasattr(msg.errorMsg, 'args'):
                    print('errorMsg detected', msg.errorMsg.args[0])
                    if msg.errorMsg.args[0] == 10054:
                        print('Ending IB Framework instance')
                        self.cerebro.runstop()

            if hasattr(msg, 'errorCode'):
                print('errorCode detected', getattr(msg, 'errorCode'))
                if getattr(msg, 'errorCode') == 504:
                    print('errorCode 504')
                    print('Ending IB Framework instance')
                    self.cerebro.runstop()
                elif getattr(msg, 'errorCode') == 1100:
                    print('errorCode 1100')
                    print('Ending IB Framework instance')
                    self.cerebro.runstop()

            # if "errorMsg=Not connected" in msg:
            #    connection = False
            # jsonfile.updateFile("strategy", self.strategy, {"Error": connection})

    def notify_timer(self, timer, when, *args, **kwargs):
        print("*******************************************************")
        print(f'strategy notify_timer with tid {timer.p.tid}')
        print("*********************RESTARTING********************")
        #self.restart()

    def __init__(self):
        # Load config values
        self.strategy = self.p.strat
        self.mode = config.params.main.mode
        self.contractNames = config.get_contract_names_by_strat(self.strategy)
        self.dualData = config.get_strategy_parameter(self.strategy, "dual_data")
        self.candleSize = config.get_strategy_parameter(self.strategy, "candle_size")
        self.longCandleSize = config.get_strategy_parameter(self.strategy, "long_candle_size")
        self.printLog = config.params.display.enabled
        self.multi = False
        self.onePlot = config.params.display.one_plot
        self.table = config.params.display.table
        self.logs = config.params.data.logs_output_dir
        self.pnl_observer = config.params.observers.pnl
        self.timezone = pytz.timezone(config.params.main.timezone)

        self.reset_time = config.params.watchdog.reset_time
        self.live_test = config.params.watchdog.live_test
        self.cycle_mult = config.params.watchdog.cycle_mult
        self.early_trading = config.params.watchdog.early_trading
        self.late_trading = config.params.watchdog.late_trading
        self.tzdata = config.params.main.timezone

        if self.mode == "live":
            self.market = config.get_strategy_parameter(self.strategy, "watchdog_market")
            self.add_reset_timer(reset_time=self.reset_time,
                                 live_test=self.live_test,
                                 cycle_mult=self.cycle_mult,
                                 market=self.market,
                                 early_trading=self.early_trading,
                                 late_trading=self.late_trading,
                                 strategy=self.strategy,
                                 tzdata=self.tzdata
                                 )

        """
        Initialize SQLite database

        """
        if self.mode != "optimize":
            try:
                self.con = sqlite3.connect('IBFramework.sqlite')
                print("Database created")
            except Error:
                print(Error)

            self.cur = self.con.cursor()

            self.cur.execute(
                "CREATE TABLE IF NOT EXISTS tradelog (Date text, Strategy text, Ref integer, Contract text, Type text, "
                "Amount real, Price real, PnL real)"
                )
            self.cur.execute(
                "CREATE TABLE IF NOT EXISTS pnl (Date text, Strategy text, Contract text, Size text, Price real, "
                "PctChange real, PnL real)"
            )
            self.cur.execute(
                "CREATE TABLE IF NOT EXISTS ohlc_short (date text PRIMARY KEY, Strategy text, open real, high real, "
                "low real, close real, volume integer)"
            )
            self.cur.execute(
                "CREATE TABLE IF NOT EXISTS ohlc_long (date text PRIMARY KEY, Strategy text, open real, high real, "
                "low real, close real, volume integer)"
            )

            self.con.commit()

            print("SQLite Tables Created")

            if self.mode != "live":
                self.cur.execute(
                    "DELETE FROM tradelog"
                    )
                self.cur.execute(
                    "DELETE FROM pnl"
                    )
                self.cur.execute(
                    "DELETE FROM ohlc_short"
                    )
                self.cur.execute(
                    "DELETE FROM ohlc_long"
                    )

                self.con.commit()


        utils.check_path(self.logs)
        filename = list()
        filename.append(self.logs)
        filename.append("tradelist")
        filenametxt = ("").join(filename)
        filenametxt = ("_").join([filenametxt, self.strategy, self.mode])
        self.filenametxt = ("").join([filenametxt, ".csv"])
        exist = utils.checkfile(self.filenametxt)
        if exist and self.mode == "live":
            self.tradelog = pd.read_csv(self.filenametxt)
        else:
            self.tradelog = pd.DataFrame()
        """
        Open or initialize a pnl database per strategy

        """
        self.new_pnl = dict()
        utils.check_path(self.logs)
        filename = list()
        filename.append(self.logs)
        filename.append("pnl")
        filenamepnl = ("").join(filename)
        filenamepnl = ("_").join([filenamepnl, self.strategy, self.mode])
        self.filenamepnl = ("").join([filenamepnl, ".csv"])
        exist = utils.checkfile(self.filenamepnl)
        if exist and self.mode == "live":
            self.pnl = pd.read_csv(self.filenamepnl, index_col="Date")
        else:
            self.pnl = pd.DataFrame(columns=["Date"])
            self.pnl.set_index("Date", inplace=True)

        '''
        Create a dictionary of indicators so that we can dynamically add the
        indicators to the strategy using a loop. This mean the strategy will
        work with any number of data feeds.
        '''
        # To keep track of pending orders and buy price/commission

        self.orders = dict()  # orders per data (main, stop, limit, manual-close)

        self.livestatus = dict()  # Live status of contracts

        self.cash = None
        self.value = None
        self.optype = None

        self.buyprice = dict()
        self.buycomm = dict()
        self.buy_authorized = dict()
        self.sell_authorized = dict()

        self.long_datas = self.datas[1::2]
        self.short_datas = self.datas[::2]

        self.long_inds = dict()
        self.short_inds = dict()

        self.contracts = dict()

        self.current_action = dict()
        self.total_pnl = dict()
        self.last_pnl = dict()
        self.multiplier = dict()
        self.sectype = dict()
        self.open_price = dict()
        self.close_price = dict()
        self.trade_amount = dict()
        self.trade_open = dict()
        self.trade_commission = dict()
        self.datastatus = dict()
        self.last_candle_timestamp = dict()
        self.round = 0

        # json creation
        if self.mode == "live":
            self.json = dict()
            self.json[self.p.strat] = dict()

        for i, d in enumerate(self.long_datas):

            # Initializing contract specific variables
            self.contracts[d] = d._name.split('-')[0]
            self.multiplier[d] = config.get_contract_parameter(self.contracts[d], "multiplier")
            self.sectype[d] = config.get_contract_parameter(self.contracts[d], "sectype")
            self.buy_authorized[d] = False
            self.sell_authorized[d] = False
            self.current_action[d] = "Initializing"

            if self.mode == 'optimize':
                self.percentile_open = self.p.percentile_open
                self.percentile_close = self.p.percentile_close
                self.lowpercentile = self.p.lowpercentile
                self.highpercentile = self.p.highpercentile
                self.percentile_period = self.p.percentile_period
                self.bb_open = self.p.bb_open
                self.bb_close = self.p.bb_close
                self.low_bb = self.p.low_bb
                self.high_bb = self.p.high_bb
                self.bb_period = self.p.bb_period
                self.dev_factor = self.p.dev_factor
                self.macd_open = self.p.macd_open
                self.macd_close = self.p.macd_close
                self.macd1 = self.p.macd1
                self.macd2 = self.p.macd2
                self.macdsig = self.p.macdsig
                self.atr_close = self.p.atr_close
                self.atrperiod = self.p.atrperiod
                self.atrdist = self.p.atrdist
                self.atrdist_trail = self.p.atrdist_trail
                self.mean_return_close = self.p.mean_return_close
                self.trend_open = self.p.trend_open
                self.trend_close = self.p.trend_close
                self.trend = self.p.trend
                self.trend_strength = self.p.trend_strength
                self.smaperiod = self.p.smaperiod
                self.dirperiod = self.p.dirperiod
                self.osc_open = self.p.osc_open
                self.osc_close = self.p.osc_close
                self.osc_ob = self.p.osc_ob
                self.osc_os = self.p.osc_os
                self.osc_ob_exit = self.p.osc_ob_exit
                self.osc_os_exit = self.p.osc_os_exit
                self.osc_period = self.p.osc_period
                self.oscillator = self.p.oscillator

            else:
                self.percentile_open = config.get_contract_parameter(self.contracts[d], "percentile_open")
                self.percentile_close = config.get_contract_parameter(self.contracts[d], "percentile_close")
                self.lowpercentile = config.get_contract_parameter(self.contracts[d], "lowpercentile")
                self.highpercentile = config.get_contract_parameter(self.contracts[d], 'highpercentile')
                self.percentile_period = config.get_contract_parameter(self.contracts[d], 'percentile_period')
                self.bb_open = config.get_contract_parameter(self.contracts[d], 'bb_open')
                self.bb_close = config.get_contract_parameter(self.contracts[d], 'bb_close')
                self.low_bb = config.get_contract_parameter(self.contracts[d], 'low_bb')
                self.high_bb = config.get_contract_parameter(self.contracts[d], 'high_bb')
                self.bb_period = config.get_contract_parameter(self.contracts[d], 'bb_period')
                self.dev_factor = config.get_contract_parameter(self.contracts[d], 'dev_factor')
                self.macd_open = config.get_contract_parameter(self.contracts[d], 'macd_open')
                self.macd_close = config.get_contract_parameter(self.contracts[d], 'macd_close')
                self.macd1 = config.get_contract_parameter(self.contracts[d], 'macd1')
                self.macd2 = config.get_contract_parameter(self.contracts[d], 'macd2')
                self.macdsig = config.get_contract_parameter(self.contracts[d], 'macdsig')
                self.atr_close = config.get_contract_parameter(self.contracts[d], 'atr_close')
                self.atrperiod = config.get_contract_parameter(self.contracts[d], 'atrperiod')
                self.atrdist = config.get_contract_parameter(self.contracts[d], 'atrdist')
                self.atrdist_trail = config.get_contract_parameter(self.contracts[d], 'atrdist_trail')
                self.mean_return_close = config.get_contract_parameter(self.contracts[d], 'mean_return_close')
                self.trend_open = config.get_contract_parameter(self.contracts[d], 'trend_open')
                self.trend_close = config.get_contract_parameter(self.contracts[d], 'trend_close')
                self.trend = config.get_contract_parameter(self.contracts[d], 'trend')
                self.trend_strength = config.get_contract_parameter(self.contracts[d], 'trend_strength')
                self.smaperiod = config.get_contract_parameter(self.contracts[d], 'smaperiod')
                self.dirperiod= config.get_contract_parameter(self.contracts[d], 'dirperiod')
                self.osc_open = config.get_contract_parameter(self.contracts[d], 'osc_open')
                self.osc_close = config.get_contract_parameter(self.contracts[d], 'osc_close')
                self.osc_ob = config.get_contract_parameter(self.contracts[d], 'osc_ob')
                self.osc_os = config.get_contract_parameter(self.contracts[d], 'osc_os')
                self.osc_ob_exit = config.get_contract_parameter(self.contracts[d], 'osc_ob_exit')
                self.osc_os_exit = config.get_contract_parameter(self.contracts[d], 'osc_os_exit')
                self.osc_period = config.get_contract_parameter(self.contracts[d], 'osc_period')
                self.oscillator = config.get_contract_parameter(self.contracts[d], 'oscillator')

            # Initializing contract Json
            if self.mode == "live":
                self.json[self.p.strat][d] = dict()
                self.json[self.p.strat][d]['pstop'] = dict()

            # Initializing indicators
            self.long_inds[d] = dict()
            # Keep a reference to the "open","close","high","low" lines in the data[0] dataseries
            self.long_inds[d]['dataopen'] = d.open
            self.long_inds[d]['dataclose'] = d.close
            self.long_inds[d]['datahigh'] = d.high
            self.long_inds[d]['datalow'] = d.low

            # Initialize pstop
            self.long_inds[d]['pstop'] = None
            self.long_inds[d]['future_pstop'] = None

            # Calculate Percentage Change indicator
            self.long_inds[d]['pct'] = bt.indicators.PctChange(d, period=1)

            # Calculate MACD indicators
            macd_in_strategy = self.macd_open or self.macd_close
            if macd_in_strategy:
                self.long_inds[d]['macd'] = bt.indicators.MACD(d,
                                                               period_me1=self.macd1,
                                                               period_me2=self.macd2,
                                                               period_signal=self.macdsig,
                                                               subplot=True,
                                                               plot=True
                                                               )
                self.long_inds[d]['mcross'] = bt.indicators.CrossOver(self.long_inds[d]['macd'].macd,
                                                                      self.long_inds[d]['macd'].signal,
                                                                      plot=False
                                                                      )

            # Calculate ATR indicator
            atr_in_strategy = self.atr_close
            if atr_in_strategy:
                self.long_inds[d]['atr'] = bt.indicators.AverageTrueRange(d,
                                                                          period=self.atrperiod,
                                                                          subplot=True,
                                                                          plot=True
                                                                          )

            # Calculate trend indicators
            trend_in_strategy = self.trend_open or self.trend_close
            if trend_in_strategy:
                # Calculate SMAdir indicator
                if self.trend == "SMA":
                    self.long_inds[d]['smadir'] = indicators.SMAdir(d,
                                                                    period=self.smaperiod,
                                                                    dirperiod=self.dirperiod,
                                                                    plot=False
                                                                    )
                # Calculate DMI indicator
                elif self.trend == "DMI":
                    self.long_inds[d]['dmi'] = bt.indicators.DirectionalMovementIndex(d,
                                                                                      period=self.smaperiod,
                                                                                      safediv=True,
                                                                                      plot=False
                                                                                      )
                # Calculate Supertrend indicator
                elif self.trend == "STR":
                    self.long_inds[d]['super'] = bt.indicators.SuperTrend(d,
                                                                          period=self.smaperiod,
                                                                          multiplier=self.atrdist,
                                                                          plot=False
                                                                         )
            # Calculate percentile indicator
            percentile_in_strategy = self.percentile_open or self.percentile_close
            if percentile_in_strategy:
                self.long_inds[d]['percentile_indicator'] = indicators.StatPercentile(d,
                                                                                      lowpercentile=self.lowpercentile,
                                                                                      highpercentile=self.highpercentile,
                                                                                      percentile_period=self.percentile_period,
                                                                                      contract=self.contracts[d],
                                                                                      candle_size=self.longCandleSize,
                                                                                      subplot=False,
                                                                                      plot=True
                                                                                      )
                self.long_inds[d]['plcross'] = bt.indicators.CrossDown(self.long_inds[d]['dataclose'],
                                                                       self.long_inds[d]['percentile_indicator'].lines.long,
                                                                       plot=False)
                self.long_inds[d]['pscross'] = bt.indicators.CrossUp(self.long_inds[d]['dataclose'],
                                                                     self.long_inds[d]['percentile_indicator'].lines.short,
                                                                     plot=False)
            # Calculate bollinger bands indicators
            bb_in_strategy = self.bb_open or self.bb_close
            if bb_in_strategy:
                self.long_inds[d]['bb'] = bt.indicators.BollingerBandsPct(d,
                                                                           period=self.bb_period,
                                                                           devfactor=self.dev_factor,
                                                                           safediv=True,
                                                                           subplot=False,
                                                                           plot=False
                                                                          )
                self.long_inds[d]['bb_std'] = bt.indicators.BollingerBands(d,
                                                                           period=self.bb_period,
                                                                           devfactor=self.dev_factor,
                                                                           subplot=False,
                                                                           plot=True
                                                                       )
                self.long_inds[d]['bblcross'] = bt.indicators.CrossUp(self.long_inds[d]['bb'].pctb,
                                                                      (-self.low_bb/100),
                                                                      plot=False
                                                                      )
                self.long_inds[d]['bbscross'] = bt.indicators.CrossDown(self.long_inds[d]['bb'].pctb,
                                                                        (self.high_bb/100),
                                                                        plot=False
                                                                        )
                self.long_inds[d]['bbmcross'] = bt.indicators.CrossOver(self.long_inds[d]['dataclose'],
                                                                        self.long_inds[d]['bb'].mid,
                                                                        plot=False
                                                                        )
            # Adds oscillators
            osc_in_strategy = self.osc_open or self.osc_close
            if osc_in_strategy:
                # Calculate RSI indicator
                if self.oscillator == "RSI":
                    self.long_inds[d]["osc"] = bt.indicators.RSI(d, period=self.osc_period, upperband=self.osc_ob,
                                                                 lowerband=self.osc_os, safediv=True, plot=True)
                # Calculate MFI indicator
                elif self.oscillator == "MFI":
                    self.long_inds[d]["osc"] = indicators.MoneyFlowIndicator(d, period=self.osc_period, plot=True)
                # Calculate STOCH indicator
                elif self.oscillator == "STOCH":
                    self.long_inds[d]["osc"] = bt.indicators.StochasticFast(d,
                                                                            period=self.osc_period,
                                                                            upperband=self.osc_ob,
                                                                            lowerband=self.osc_os,
                                                                            safediv=True,
                                                                            plot=True
                                                                            )
                # Opening Oscillator results
                self.long_inds[d]["osc_cross_long"] = self.long_inds[d]["osc"] < self.osc_os
                self.long_inds[d]["osc_cross_short"] = self.long_inds[d]["osc"] > self.osc_ob

                # Exit Oscillator results
                self.long_inds[d]["osc_cross_long_exit"] = self.long_inds[d]["osc"] < self.osc_os_exit
                self.long_inds[d]["osc_cross_short_exit"] = self.long_inds[d]["osc"] > self.osc_ob_exit

            # Add Retrun to mean SMA Exit
            if self.mean_return_close:
                self.long_inds[d]['sma'] = bt.indicators.SMA(d,
                                                             period=self.smaperiod,
                                                             subplot=False,
                                                             plot=True
                                                             )
                self.long_inds[d]["SMA_long_exit"] = self.long_inds[d]["sma"] < self.long_inds[d]['dataclose']
                self.long_inds[d]["SMA_short_exit"] = self.long_inds[d]["sma"] > self.long_inds[d]['dataclose']

            if i > 0: # Check we are not on the first loop of data feed:
                if self.onePlot:
                    d.plotinfo.plotmaster = self.datas[0]

        for i, d in enumerate(self.short_datas):

            # Initializing contract specific variables
            self.contracts[d] = d._name.split('-')[0]
            self.total_pnl[d] = 0
            self.last_pnl[d] = 0
            self.open_price[d] = None
            self.close_price[d] = None
            self.trade_amount[d] = None
            self.trade_open[d] = None
            self.trade_commission[d] = None
            self.datastatus[d] = None
            self.last_candle_timestamp[d] = None

            if self.mode == 'optimize':
                self.percentile_open = self.p.percentile_open
                self.percentile_close = self.p.percentile_close
                self.lowpercentile = self.p.lowpercentile
                self.highpercentile = self.p.highpercentile
                self.percentile_period = self.p.percentile_period
                self.bb_open = self.p.bb_open
                self.bb_close = self.p.bb_close
                self.low_bb = self.p.low_bb
                self.high_bb = self.p.high_bb
                self.bb_period = self.p.bb_period
                self.dev_factor = self.p.dev_factor
                self.macd_open = self.p.macd_open
                self.macd_close = self.p.macd_close
                self.macd1 = self.p.macd1
                self.macd2 = self.p.macd2
                self.macdsig = self.p.macdsig
                self.atr_close = self.p.atr_close
                self.atrperiod = self.p.atrperiod
                self.atrdist = self.p.atrdist
                self.atrdist_trail = self.p.atrdist_trail
                self.mean_return_close = self.p.mean_return_close
                self.trend_open = self.p.trend_open
                self.trend_close = self.p.trend_close
                self.trend = self.p.trend
                self.trend_strength = self.p.trend_strength
                self.smaperiod = self.p.smaperiod
                self.dirperiod = self.p.dirperiod
                self.osc_open = self.p.osc_open
                self.osc_close = self.p.osc_close
                self.osc_ob = self.p.osc_ob
                self.osc_os = self.p.osc_os
                self.osc_ob_exit = self.p.osc_ob_exit
                self.osc_os_exit = self.p.osc_os_exit
                self.osc_period = self.p.osc_period
                self.oscillator = self.p.oscillator

            else:
                self.percentile_open = config.get_contract_parameter(self.contracts[d], "percentile_open")
                self.percentile_close = config.get_contract_parameter(self.contracts[d], "percentile_close")
                self.lowpercentile = config.get_contract_parameter(self.contracts[d], "lowpercentile")
                self.highpercentile = config.get_contract_parameter(self.contracts[d], 'highpercentile')
                self.percentile_period = config.get_contract_parameter(self.contracts[d], 'percentile_period')
                self.bb_open = config.get_contract_parameter(self.contracts[d], 'bb_open')
                self.bb_close = config.get_contract_parameter(self.contracts[d], 'bb_close')
                self.low_bb = config.get_contract_parameter(self.contracts[d], 'low_bb')
                self.high_bb = config.get_contract_parameter(self.contracts[d], 'high_bb')
                self.bb_period = config.get_contract_parameter(self.contracts[d], 'bb_period')
                self.dev_factor = config.get_contract_parameter(self.contracts[d], 'dev_factor')
                self.macd_open = config.get_contract_parameter(self.contracts[d], 'macd_open')
                self.macd_close = config.get_contract_parameter(self.contracts[d], 'macd_close')
                self.macd1 = config.get_contract_parameter(self.contracts[d], 'macd1')
                self.macd2 = config.get_contract_parameter(self.contracts[d], 'macd2')
                self.macdsig = config.get_contract_parameter(self.contracts[d], 'macdsig')
                self.atr_close = config.get_contract_parameter(self.contracts[d], 'atr_close')
                self.atrperiod = config.get_contract_parameter(self.contracts[d], 'atrperiod')
                self.atrdist = config.get_contract_parameter(self.contracts[d], 'atrdist')
                self.atrdist_trail = config.get_contract_parameter(self.contracts[d], 'atrdist_trail')
                self.mean_return_close = config.get_contract_parameter(self.contracts[d], 'mean_return_close')
                self.trend_open = config.get_contract_parameter(self.contracts[d], 'trend_open')
                self.trend_close = config.get_contract_parameter(self.contracts[d], 'trend_close')
                self.trend = config.get_contract_parameter(self.contracts[d], 'trend')
                self.trend_strength = config.get_contract_parameter(self.contracts[d], 'trend_strength')
                self.smaperiod = config.get_contract_parameter(self.contracts[d], 'smaperiod')
                self.dirperiod= config.get_contract_parameter(self.contracts[d], 'dirperiod')
                self.osc_open = config.get_contract_parameter(self.contracts[d], 'osc_open')
                self.osc_close = config.get_contract_parameter(self.contracts[d], 'osc_close')
                self.osc_ob = config.get_contract_parameter(self.contracts[d], 'osc_ob')
                self.osc_os = config.get_contract_parameter(self.contracts[d], 'osc_os')
                self.osc_ob_exit = config.get_contract_parameter(self.contracts[d], 'osc_ob_exit')
                self.osc_os_exit = config.get_contract_parameter(self.contracts[d], 'osc_os_exit')
                self.osc_period = config.get_contract_parameter(self.contracts[d], 'osc_period')
                self.oscillator = config.get_contract_parameter(self.contracts[d], 'oscillator')

            # Initializing indicators
            self.short_inds[d] = dict()
            # Keep a reference to the "open","close","high","low" lines in the data[0] dataseries
            self.short_inds[d]['dataopen'] = d.open
            self.short_inds[d]['dataclose'] = d.close
            self.short_inds[d]['datahigh'] = d.high
            self.short_inds[d]['datalow'] = d.low

            # Initialize pstop
            self.short_inds[d]['pstop'] = None
            self.short_inds[d]['future_pstop'] = None

            # Calculate Percentage Change indicator
            self.short_inds[d]['pct'] = bt.indicators.PctChange(d, period=1)

            # Calculate MACD indicators
            macd_in_strategy = self.macd_open or self.macd_close
            if macd_in_strategy:
                self.short_inds[d]['macd'] = bt.indicators.MACD(d,
                                                                period_me1=self.macd1,
                                                                period_me2=self.macd2,
                                                                period_signal=self.macdsig,
                                                                subplot=True,
                                                                plot=True
                                                                )
                self.short_inds[d]['mcross'] = bt.indicators.CrossOver(self.short_inds[d]['macd'].macd,
                                                                       self.short_inds[d]['macd'].signal,
                                                                       plot=False
                                                                       )
            # Calculate ATR indicator
            atr_in_strategy = self.atr_close
            if atr_in_strategy:
                self.short_inds[d]['atr'] = bt.indicators.AverageTrueRange(d,
                                                                           period=self.atrperiod,
                                                                           subplot=True,
                                                                           plot=True
                                                                           )

            # Calculate trend indicators
            trend_in_strategy = self.trend_open or self.trend_close
            if trend_in_strategy:
                # Calculate SMAdir indicator
                if self.trend == "SMA":
                    self.short_inds[d]['smadir'] = indicators.SMAdir(d,
                                                                     period=self.smaperiod,
                                                                     dirperiod=self.dirperiod,
                                                                     plot=False
                                                                     )
                # Calculate DMI indicator
                elif self.trend == "DMI":
                    self.short_inds[d]['dmi'] = bt.indicators.DirectionalMovementIndex(d,
                                                                                       period=self.smaperiod,
                                                                                       safediv=True,
                                                                                       plot=False
                                                                                      )
                # Calculate Supertrend indicator
                elif self.trend == "STR":
                    self.short_inds[d]['super'] = bt.indicators.SuperTrend(d,
                                                                           period=self.smaperiod,
                                                                           multiplier=self.atrdist,
                                                                           plot=False
                                                                          )
            # Calculate percentile indicator
            percentile_in_strategy = self.percentile_open or self.percentile_close
            if percentile_in_strategy:
                self.short_inds[d]['percentile_indicator'] = indicators.StatPercentile(d,
                                                                                       lowpercentile=self.lowpercentile,
                                                                                       highpercentile=self.highpercentile,
                                                                                       percentile_period = self.percentile_period,
                                                                                       contract=self.contracts[d],
                                                                                       candle_size=self.candleSize,
                                                                                       subplot=False,
                                                                                       plot=True
                                                                                       )
                self.short_inds[d]['plcross'] = bt.indicators.CrossDown(self.short_inds[d]['dataclose'],
                                                                        self.short_inds[d]['percentile_indicator'].lines.long,
                                                                        plot=False)
                self.short_inds[d]['pscross'] = bt.indicators.CrossUp(self.short_inds[d]['dataclose'],
                                                                      self.short_inds[d]['percentile_indicator'].lines.short,
                                                                      plot=False)
            # Calculate bollinger bands indicators
            bb_in_strategy = self.bb_open or self.bb_close
            if bb_in_strategy:
                self.short_inds[d]['bb'] = bt.indicators.BollingerBandsPct(d,
                                                                           period=self.bb_period,
                                                                           devfactor=self.dev_factor,
                                                                           safediv=True,
                                                                           subplot=False,
                                                                           plot=False
                                                                           )
                self.short_inds[d]['bb_std'] = bt.indicators.BollingerBands(d,
                                                                           period=self.bb_period,
                                                                           devfactor=self.dev_factor,
                                                                           subplot=False,
                                                                           plot=True
                                                                            )
                self.short_inds[d]['bblcross'] = bt.indicators.CrossDown(self.short_inds[d]['bb'].pctb,
                                                                         (-self.low_bb/100),
                                                                        plot=False)
                self.short_inds[d]['bbscross'] = bt.indicators.CrossUp(self.short_inds[d]['bb'].pctb,
                                                                       (self.high_bb / 100),
                                                                      plot=False)
                self.short_inds[d]['bbmcross'] = bt.indicators.CrossOver(self.short_inds[d]['dataclose'],
                                                                       self.short_inds[d]['bb'].mid,
                                                                       plot=False
                                                                       )
            # Adds oscillators
            osc_in_strategy = self.osc_open or self.osc_close
            if osc_in_strategy:
                # Calculate RSI indicator
                if self.oscillator == "RSI":
                    self.short_inds[d]["osc"] = bt.indicators.RSI(d, period=self.osc_period, upperband=self.osc_ob,
                                                                 lowerband=self.osc_os, safediv=True, plot=True)
                # Calculate MFI indicator
                elif self.oscillator == "MFI":
                    self.short_inds[d]["osc"] = indicators.MoneyFlowIndicator(d, period=self.osc_period, plot=True)
                # Calculate STOCH indicator
                elif self.oscillator == "STOCH":
                    self.short_inds[d]["osc"] = bt.indicators.StochasticFast(d,
                                                                             period=self.osc_period,
                                                                             upperband=self.osc_ob,
                                                                             lowerband=self.osc_os,
                                                                             safediv=True,
                                                                             plot=True
                                                                             )

                # Opening Oscillator results
                self.short_inds[d]["osc_cross_long"] = self.short_inds[d]["osc"] < self.osc_os
                self.short_inds[d]["osc_cross_short"] = self.short_inds[d]["osc"] > self.osc_ob

                # Exit Oscillator results
                self.short_inds[d]["osc_cross_long_exit"] = self.short_inds[d]["osc"] < self.osc_os_exit
                self.short_inds[d]["osc_cross_short_exit"] = self.short_inds[d]["osc"] > self.osc_ob_exit

            # Add Retrun to mean SMA Exit
            if self.mean_return_close:
                self.short_inds[d]['sma'] = bt.indicators.SMA(d,
                                                              period=self.smaperiod,
                                                              subplot=False,
                                                              plot=True
                                                              )
                self.short_inds[d]["SMA_long_exit"] = self.short_inds[d]["sma"] < self.short_inds[d]['dataclose'] #[0]
                self.short_inds[d]["SMA_short_exit"] = self.short_inds[d]["sma"] > self.short_inds[d]['dataclose'] #[0]

            if i > 0:  # Check we are not on the first loop of data feed:
                if self.onePlot:
                    d.plotinfo.plotmaster = self.datas[0]

        # Initializing Counttostop and datastatus for live
        if self.mode == "live":
            self.counttostop = 0
            self.datastatus[d] = 0

    def notify_cashvalue(self, cash, value):

        self.cash = cash
        self.value = value

    def notify_order(self, order):

        dt, dn = self.datetime.date(), order.data._name
        if self.mode != 'optimize':
            print('{} {} Order {} Status {} Price {}'.format(
                dt, dn, order.ref, order.getstatusname(),order.executed.price))

        if order.status in [order.Submitted, order.Accepted]:
            # Buy/Sell order submitted/accepted to/by broker - Nothing to do
            return

        # Check if an order has been completed
        # Attention: broker could reject order if not enough cash
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(
                    'BUY EXECUTED, Size: %.2f, Price: %.2f, Cost: %.2f, Comm %.2f' %
                    (order.executed.size,
                     order.executed.price,
                     order.executed.value,
                     order.executed.comm))

                self.buyprice[order.data] = order.executed.price
                self.buycomm[order.data] = order.executed.comm
            else:  # Sell
                self.log('SELL EXECUTED, Size: %.2f, Price: %.2f, Cost: %.2f, Comm %.2f' %
                         (order.executed.size,
                          order.executed.price,
                          order.executed.value,
                          order.executed.comm))

            self.bar_executed = len(self)
            self.buyprice[order.data] = order.executed.price
            self.buycomm[order.data] = order.executed.comm

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('Order Canceled/Margin/Rejected')

        # Write down: no pending order
        self.orders[order.data] = None

        if self.printLog and self.mode != 'optimize':
            print('-' * 50, 'ORDER BEGIN', datetime.now())
            print(order)
            print('-' * 50, 'ORDER END')

    def notify_trade(self, trade):

        def save_trades(trade_values):
            name = trade.data._name.split("-")[0]
            for v in trade_values:
                jsonfile.updateFile("contract", name, v)

        dt = self.data.datetime.datetime()

        if not trade.isclosed:
            self.trade_open[trade.data] = trade.price
            self.trade_amount[trade.data] = trade.size
            self.trade_commission[trade.data] = trade.commission
            if self.mode == "live":
                values = [{"price": trade.price}, {"size": trade.size}, {"commission": trade.commission}]
                save_trades(values)
            #return

        self.log('OPERATION PROFIT, GROSS %.2f, NET %.2f' %
                 (trade.pnl, trade.pnlcomm))

        if trade.isclosed:
            self.total_pnl[trade.data] += trade.pnl
            self.last_pnl[trade.data] = trade.pnl
            self.open_price[trade.data]= self.trade_open[trade.data]
            self.close_price[trade.data] = trade.price
            self.optype = "close"
            # print("trade list", trade.history)

            if self.printLog and self.mode != 'optimize':
                print('---------------------------------- TRADE ---------------------------------------')
                print("1: Data Name:                            {}".format(trade.data._name))
                print("2: Bar Num:                              {}".format(len(trade.data)))
                print("3: Current date:                         {}".format(dt))
                print('4: Status:                               Trade Complete')
                print('5: Ref:                                  {}'.format(trade.ref))
                print('6: PnL:                                  {}'.format(round(trade.pnl, 2)))
                print('--------------------------------------------------------------------------------')

                print('-' * 50, 'TRADE BEGIN', datetime.now())
                print(trade)
                print('-' * 50, 'TRADE END')

        if self.mode != "optimize":
            # Store trade data in SQLite table
            dt_str = dt.strftime('%Y-%m-%d %H:%M:%S')
            new_row_sql = (dt_str, self.strategy, trade.ref, trade.data._name, self.optype, trade.size, trade.price, trade.pnl,)
            self.cur.execute("INSERT INTO tradelog VALUES (?, ?, ?, ?, ?, ?, ?, ?)", new_row_sql)
            self.con.commit()

    def strategy(self, datas, order_type):
        pass


    def order(self, datas, type, order):
        """
        sectype = datas._name
        if self.mode == "live":
            orderkwargs = dict(data=datas,
                               exectype=bt.Order.Limit,
                               price=utils.ibPricing(datas)["ask"],
                               valid=bt.date2num(datetime.now() + timedelta(minutes=5))
                               )
            if type == "sell":
                if invert:
                orderkwargs["price"] = utils.ibPricing(datas)["ask"],
            else:
                orderkwargs["price"] = utils.ibPricing(datas)["bid"],
            if sectype == "STK":
                orderkwargs["outsideRth"] = True
        else:
            orderkwargs = dict(data=e)
        return orderkwargs
        """
        pass

    def start(self):
        # initialize pending orders dictionary
        for i, e in enumerate(self.short_datas):
            self.orders[e] = None  # sentinel to avoid operations on pending order
            self.buyprice[e] = None # Initialize buyprice on every data
            """
            if self.mode == "live":
                self.livestatus[e] = utils.ibPricing(e._name)["islive"]
            """

    def prenext(self):
        self.next(frompre=True)

    def next(self, frompre=False):

        self.round += 1

        for i, (d, e) in enumerate(zip(self.long_datas, self.short_datas)):
            if self.mode == "live":
                if self.datastatus:
                    #self.livestatus[e] = utils.ibPricing(e._name)["islive"]
                    pass

                try:
                    candle_timestamp = e.datetime.datetime(0)
                except IndexError:
                    candle_timestamp = None
                if self.last_candle_timestamp[e] is None:
                    self.last_candle_timestamp[e] = candle_timestamp
                elif self.last_candle_timestamp[e] == candle_timestamp:
                    print("****This candle had been running before...Skipping***")
                    continue
                else:
                    self.last_candle_timestamp[e] = candle_timestamp
                    print("****Updating last candle datetime***")

                """
                try:
                    print("Actual Data", e.datetime.datetime(0), e.datetime.datetime(-1), e.datetime[0], e[0], e[-1])
                except:
                    pass
                """

                if not self.livestatus[e]:
                    print(f"Back-filling or Skipping {self.datetime.datetime()} {e._name} "
                          f"contract cycle as it may not be live")
                    continue
                else:
                    print(f"{e._name} is live... proceeding with cycle")
            dt, (dn, en) = self.datetime.datetime(), (d._name, e._name)
            pos = self.getposition(e).size
            if self.mode != 'optimize':
                print('{} {} {} Position {}'.format(dt, dn, en, pos), end="")

            # Check if an order is pending ... if yes, we cannot send a 2nd one
            if self.orders[e]:
                print(f"Skipping {e._name} contract cycle due to pending order")
                return  # pending order execution

            # Simply log the closing price of the series from the reference
            # self.log('Close, %.2f' % self.long_inds[d]['dataclose'][0])

            if self.mode == "live":
                if self.counttostop:  # stop after x live lines
                    self.counttostop -= 1
                    if not self.counttostop:
                        self.env.runstop()
                        return

            # Calculate variables
            multiplier = self.multiplier[d]
            sectype = self.sectype[d]
            buy_enabled = self.p.buy_enabled
            sell_enabled = self.p.sell_enabled
            close_enabled = self.p.close_enabled
            invert = self.p.invert
            gain_close = self.p.gain_close
            gain = self.p.gain
            stoploss_close = self.p.stoploss_close
            stoploss = self.p.stoploss
            percentile_open = self.p.percentile_open
            percentile_close = self.p.percentile_close
            bb_open = self.p.bb_open
            bb_close = self.p.bb_close
            macd_open = self.p.macd_open
            macd_close = self.p.macd_close
            atr_close = self.p.atr_close
            trail_on_buy = self.p.trail_on_buy
            mean_return_close = self.p.mean_return_close
            trend_open = self.p.trend_open
            trend_close = self.p.trend_close
            trend = self.p.trend
            trend_strength = self.p.trend_strength
            osc_open = self.p.osc_open
            osc_close = self.p.osc_close

            # Create strategy lists
            open_buy = list()
            open_sell = list()
            close_buy = list()
            close_sell = list()

            # Gain calculations
            gain_in_strategy = gain_close or stoploss_close
            if gain_in_strategy:
                if pos:
                    try:
                        if self.mode == "live":
                            name = e._name.split("-")[0]
                            self.trade_open[e] = self.getposition(e).price
                            """
                            if self.trade_open[e] is None:
                                self.trade_open[e], filepath = jsonfile.readValue("contract", name, "price")
                                if not self.trade_open[e]:
                                    # if value not found get position average value from IB
                                    self.trade_open[e] = self.getposition(e).price
                            """
                            """
                            if self.trade_amount[e] is None:
                                # Check for json file to retrieve
                                value, filepath = jsonfile.readValue("contract", name, "size")
                            """
                            self.trade_amount[e] = self.getposition(e).size
                            """
                            if self.trade_amount[e] is None:
                                # if value not found get position average value from IB
                                self.trade_amount[e] = self.getposition(e).size

                            """
                            if self.trade_commission[e] is None:
                                # Check for json file to retrieve
                                value, filepath = jsonfile.readValue("contract", name, "commission")
                                if self.trade_commission[e] is None:
                                    # if value not found set figurative value of 1
                                    self.trade_commission[e] = 1

                        pos_price = self.trade_open[e]
                        pos_size = self.trade_amount[e]
                        pos_size_sign = 1 if pos_size >= 0 else -1
                        pos_commission = self.trade_commission[e]
                        pos_total = (pos_price * pos_size / multiplier) + (pos_commission*pos_size_sign)
                        pos_expected = (self.short_inds[e]['dataclose'][0] * pos_size ) - \
                                       (pos_commission*pos_size_sign)
                        pos_gain = ((pos_expected-pos_total)/abs(pos_total)) * 100

                        gain_expected = ((gain * abs(pos_total))/100) + pos_total

                    except IndexError:
                        pos_gain = 0
                        gain_expected = 0
                else:
                    pos_gain = 0
                    gain_expected = 0
                pos_gain_close = pos_gain > gain
                pos_stoploss_close = pos_gain < -stoploss
                if gain_close:
                    close_buy.append(pos_gain_close)
                    close_sell.append(pos_gain_close)
                if stoploss_close:
                    close_buy.append(pos_stoploss_close)
                    close_sell.append(pos_stoploss_close)
            else:
                pos_gain = None
                gain_expected = None

            # Percentile calculations
            percentile_in_strategy = percentile_open or percentile_close
            if percentile_in_strategy:
                try:
                    percentile_buy = self.long_inds[d]["plcross"] > 0.0
                    percentile_sell = self.long_inds[d]["pscross"] > 0.0
                except IndexError:
                    percentile_buy = False
                    percentile_sell = False
                if percentile_open:
                    open_buy.append(percentile_buy)
                    open_sell.append(percentile_sell)
                if percentile_close:
                    close_buy.append(percentile_buy)
                    close_sell.append(percentile_sell)

            # Bollinger bands calculations
            bb_in_strategy = bb_open or bb_close
            if bb_in_strategy:
                try:
                    bb_buy = self.long_inds[d]["bblcross"] > 0.0
                    bb_sell = self.long_inds[d]["bbscross"] > 0.0
                    bb_exit = self.long_inds[d]["bbmcross"] != 0.0
                except IndexError:
                    bb_buy = False
                    bb_sell = False
                    bb_exit = False
                if bb_open:
                    open_buy.append(bb_buy)
                    open_sell.append(bb_sell)
                if bb_close:
                    close_buy.append(bb_exit)
                    close_sell.append(bb_exit)

            # MACD calculations
            macd_in_strategy = macd_open or macd_close
            if macd_in_strategy:
                try:
                    tolerance = 0.0025
                    signalDeltaPercent = self.long_inds[d]['macd'].macd - self.long_inds[d]['macd'].signal
                    #mcross_buy = signalDeltaPercent > tolerance
                    #mcross_sell = signalDeltaPercent < -tolerance
                    #print(' MACD Delta {}'.format(signalDeltaPercent), end="")


                    mcross_buy = self.long_inds[d]['mcross'][0] > 0.0
                    mcross_sell = self.long_inds[d]['mcross'][0] < 0.0
                except IndexError:
                    mcross_buy = False
                    mcross_sell = False
                if macd_open:
                    open_buy.append(mcross_buy)
                    open_sell.append(mcross_sell)
                if macd_close:
                    close_buy.append(mcross_buy)
                    close_sell.append(mcross_sell)

            # ATR calculations
            atr_in_strategy = atr_close or trail_on_buy
            if atr_in_strategy:
                try:
                    pdist = self.long_inds[d]['atr'][0] * self.atrdist
                    pdist_trail = self.long_inds[d]['atr'][0] * self.atrdist_trail
                except (IndexError, KeyError) :
                    pdist = 10000
                    pdist_trail = 10000
            #print("ATR", atr_in_strategy, atr_close, trail_on_buy)
            """
            def create_atr_trail():
                if atr_in_strategy:
                    pass
            def update_atr_trail():
                if atr_in_strategy:
                    pass

            if atr_in_strategy:
                try:
                    pdist = self.long_inds[d]['atr'][0] * self.atrdist
                    pdist_trail = self.long_inds[d]['atr'][0] * self.atrdist_trail

                    create_pstop_buy = self.long_inds[d]['dataclose'][0] - pdist
                    create_pstop_sell = self.long_inds[d]['dataclose'][0] + pdist
                    recalc_pstop_buy = self.short_inds[e]['dataclose'][0] - pdist
                    recalc_pstop_sell = self.short_inds[e]['dataclose'][0] + pdist
                except IndexError:
                    pdist = 10000
                    pdist_trail = 10000

                # If Live and when in position check if pstop is available if not retrieve it
                # from json, if json empty recalc it.
            """
            # Mean return SMA calculations
            if mean_return_close:
                try:
                    sma_buy = self.long_inds[d]["SMA_short_exit"]
                    sma_sell = self.long_inds[d]["SMA_long_exit"]
                except IndexError:
                    sma_buy = False
                    sma_sell = False
                close_buy.append(sma_buy)
                close_sell.append(sma_sell)

            # Trend calculations, enables to select between "SMA" trend and "DMI" trend
            dmi_trend = trend == "DMI"
            smadir_trend = trend == "SMA"
            super_trend = trend == "STR"
            trend_in_strategy = trend_open or trend_close
            dmi_enable = trend_in_strategy and dmi_trend
            smadir_enable = trend_in_strategy and smadir_trend
            super_enable = trend_in_strategy and super_trend
            if trend_in_strategy:
                if dmi_enable:
                    try:
                        dmi_positive_trend = self.long_inds[d]["dmi"].plusDI[0] > self.long_inds[d]["dmi"].minusDI[0]
                        dmi_negative_trend = self.long_inds[d]["dmi"].minusDI[0] > self.long_inds[d]["dmi"].plusDI[0]
                        dmi_adx_threshold = self.long_inds[d]["dmi"].adx[0] > trend_strength
                        trend_buy = dmi_adx_threshold and dmi_positive_trend
                        trend_sell = dmi_adx_threshold and dmi_negative_trend

                        #dmi_adx_threshold_close = self.long_inds[d]["dmi"].adx[0] > trend_strength + 40

                        dmi_positive_trend = self.short_inds[e]["dmi"].plusDI[0] > self.short_inds[e]["dmi"].minusDI[0]
                        dmi_negative_trend = self.short_inds[e]["dmi"].minusDI[0] > self.short_inds[e]["dmi"].plusDI[0]
                        dmi_adx_threshold_close = self.short_inds[e]["dmi"].adx[0] > trend_strength + 40
                        print("trend_strength", self.short_inds[e]["dmi"].adx[0], end="")
                        trend_close_buy = dmi_adx_threshold_close and dmi_negative_trend
                        trend_close_sell = dmi_adx_threshold_close and dmi_positive_trend

                    except IndexError:
                        trend_buy = False
                        trend_sell = False

                        trend_close_buy = False
                        trend_close_sell = False
                elif smadir_enable:
                    try:
                        trend_buy = self.long_inds[d]['smadir'][0] < 0.0
                        trend_sell = self.long_inds[d]['smadir'][0] > 0.0
                    except IndexError:
                        trend_buy = False
                        trend_sell = False
                elif super_enable:
                    try:
                        trend_buy = self.long_inds[d]["super"][0] < self.long_inds[d]['dataclose'][0]
                        trend_sell = self.long_inds[d]["super"][0] > self.long_inds[d]['dataclose'][0]
                    except IndexError:
                        trend_buy = False
                        trend_sell = False
                if trend_open:
                    open_buy.append(trend_buy)
                    open_sell.append(trend_sell)
                if trend_close:
                    close_buy.append(trend_close_buy)
                    close_sell.append(trend_close_sell)

            # Oscillator calculations, enables to select between "MFI" trend, "RSI" and "STOCH" oscillators
            osc_in_strategy = osc_open or osc_close
            if osc_in_strategy:
                try:
                    osc_cross_long = self.short_inds[e]["osc_cross_long"][0]
                    osc_cross_short = self.short_inds[e]["osc_cross_short"][0]
                    osc_cross_long_exit = self.short_inds[e]["osc_cross_long_exit"][0]
                    osc_cross_short_exit = self.short_inds[e]["osc_cross_short_exit"][0]
                except IndexError:
                    osc_cross_long = False
                    osc_cross_short = False
                    osc_cross_long_exit = False
                    osc_cross_short_exit = False
                if osc_open:
                    open_buy.append(osc_cross_long)
                    open_sell.append(osc_cross_short)
                if osc_close:
                    close_buy.append(osc_cross_long_exit)
                    close_sell.append(osc_cross_short_exit)
            """
            # Order KWARGS calculations
            if self.mode == "live":
                orderkwargs = dict(data=e,
                                   exectype=bt.Order.Limit,
                                   price=utils.ibPricing(e._name)["ask"],
                                   valid=bt.date2num(datetime.now() + timedelta(minutes=5))
                                   )
                if self.sectype[d] == "STK":
                    orderkwargs["outsideRth"] = True
            else:
                orderkwargs = dict(data=e)
            """

            # Check if we are in the market
            # if not self.position: # Open positions
            if (self.mode != "live" and not pos) or (self.mode == "live" and self.datastatus[e] and not pos):  # Open positions

                self.current_action[d] = "Waiting to open a position"

                # Not yet ... we MIGHT BUY if ...
                if buy_enabled and all(open_buy):
                    if self.p.trail_on_buy:
                        self.buy_authorized[d] = True
                    else:
                        if self.mode == "live":

                            orderkwargs = dict(data=e,
                                               exectype=bt.Order.Limit,
                                               price=utils.ibPricing(e._name)["ask"],
                                               outsideRth=True,
                                               valid=bt.date2num(datetime.now() + timedelta(minutes=5))
                                               )
                        else:

                            orderkwargs = dict(data=e)

                        self.orders[e] = self.buy(**orderkwargs)
                        self.current_action[d] = "Buying!!!"
                    if atr_in_strategy:
                        self.long_inds[d]['pstop'] = self.long_inds[d]['dataclose'][0] - pdist
                        print("***********PSTOP CREATED 2 ******************", self.long_inds[d]['pstop'])

                elif sell_enabled and all(open_sell):
                    if self.p.trail_on_buy:
                        self.sell_authorized[d] = True
                    else:
                        if self.mode == "live":

                            orderkwargs = dict(data=e,
                                               exectype=bt.Order.Limit,
                                               price=utils.ibPricing(e._name)["bid"],
                                               outsideRth=True,
                                               valid=bt.date2num(datetime.now() + timedelta(minutes=5))
                                               )
                        else:

                            orderkwargs = dict(data=e)

                        self.orders[e] = self.sell(**orderkwargs)
                        self.current_action[d] = "Selling!!!"
                    if atr_in_strategy:
                        self.long_inds[d]['pstop'] = self.long_inds[d]['dataclose'][0] + pdist
                        print("***********PSTOP CREATED 2 ******************", self.long_inds[d]['pstop'])

                elif self.buy_authorized[d]:

                    pclose = self.short_inds[e]['dataclose'][0]
                    pstop = self.long_inds[d]['pstop']
                    trigger = pclose > pstop

                    if trigger:

                        if self.mode == "live":

                            orderkwargs = dict(data=e,
                                               exectype=bt.Order.Limit,
                                               price=utils.ibPricing(e._name)["ask"],
                                               outsideRth=True,
                                               valid=bt.date2num(datetime.now() + timedelta(minutes=5))
                                               )
                        else:

                            orderkwargs = dict(data=e)

                        self.orders[e] = self.buy(**orderkwargs)  # stop met - get out

                        self.buy_authorized[d] = False
                        self.current_action[d] = "Buying!!!"

                    else:
                        self.current_action[d] = "Trailing to buy"

                        # Update only if greater than
                        self.long_inds[d]['pstop'] = min(pstop, pclose + pdist_trail)
                        if self.printLog and self.mode != 'optimize':
                            print("ATR Trail Stop Loss Short =", self.long_inds[d]['pstop'])
                        return

                elif self.sell_authorized[d]:  # On an existing long position

                    pclose = self.short_inds[e]['dataclose'][0]
                    pstop = self.long_inds[d]['pstop']
                    trigger = pclose < pstop

                    if trigger:

                        if self.mode == "live":

                            orderkwargs = dict(data=e,
                                               exectype=bt.Order.Limit,
                                               price=utils.ibPricing(e._name)["bid"],
                                               outsideRth=True,
                                               valid=bt.date2num(datetime.now() + timedelta(minutes=5))
                                               )
                        else:

                            orderkwargs = dict(data=e)

                        self.orders[e] = self.sell(**orderkwargs)  # stop met - get out

                        self.sell_authorized[d] = False
                        self.current_action[d] = "Selling!!!"

                    else:
                        self.current_action[d] = "Trailing to sell"

                        # Update only if greater than
                        self.long_inds[d]['pstop'] = max(pstop, pclose - pdist_trail)
                        if self.printLog and self.mode != 'optimize':
                            print("ATR Trail Stop Loss Long =", self.long_inds[d]['pstop'])
                        return

            elif (self.mode != "live" and pos) or (self.mode == "live" and self.datastatus[e] and pos):  # in the market # Exits

                self.current_action[d] = "Waiting to close"

                if pos > 0:  # On an existing long position
                    if atr_in_strategy:
                        if self.mode == "live":
                            if self.long_inds[d]['pstop'] is None:

                                name = d._name.split("-")[0]
                                value, filepath = jsonfile.readValue("contract", name, "pstop")
                                if not value:

                                    self.long_inds[d]['pstop'] = self.short_inds[e]['dataclose'][0] - pdist
                                    print('Pstop not found in file, recalculated Pstop=', self.long_inds[d]['pstop'])

                                else:

                                    print('Pstop correctly restored from file =', filepath, 'Pstop value Found= ',
                                          value, 'Replacing pstop current value=', self.long_inds[d]['pstop'])
                                    if value is not None:
                                        self.long_inds[d]['pstop'] = value

                        pclose = self.short_inds[e]['dataclose'][0]
                        pstop = self.long_inds[d]['pstop']
                        print("pclose", pclose, " pstop", pstop)
                        trigger = pclose < pstop
                        close_sell.append(trigger)

                    if close_enabled and any(close_sell):
                        if self.mode == "live":

                            orderkwargs = dict(data=e,
                                               exectype=bt.Order.Limit,
                                               price=utils.ibPricing(e._name)["bid"],
                                               outsideRth=True,
                                               valid=bt.date2num(datetime.now() + timedelta(minutes=5))
                                               )
                        else:

                            orderkwargs = dict(data=e)

                        self.orders[e] = self.close(**orderkwargs)  # stop met - get out
                        self.current_action[d] = "Closing!!!"


                    else:
                        self.current_action[d] = "Waiting to close"
                        if atr_in_strategy:
                            # Update only if greater than
                            self.long_inds[d]['pstop'] = max(pstop, pclose - pdist)
                            if self.printLog and self.mode != 'optimize':
                                print("ATR Trail Stop Loss Long =", self.long_inds[d]['pstop'])
                            #return

                elif pos < 0:  # On an existing short position
                    if atr_in_strategy:
                        if self.mode == "live":

                            if self.long_inds[d]['pstop'] is None:

                                name = d._name.split("-")[0]
                                value, filepath = jsonfile.readValue("contract", name, "pstop")
                                if not value:

                                    self.long_inds[d]['pstop'] = self.short_inds[e]['dataclose'][0] + pdist
                                    print('Pstop not found in file, recalculated Pstop=', self.long_inds[d]['pstop'])

                                else:

                                    print('Pstop correctly restored from file =', filepath, 'Pstop value Found= ',
                                          value, 'Replacing pstop current value=', self.long_inds[d]['pstop'])
                                    if value is not None:
                                        self.long_inds[d]['pstop'] = value

                        pclose = self.short_inds[e]['dataclose'][0]
                        pstop = self.long_inds[d]['pstop']
                        trigger = pclose > pstop
                        close_buy.append(trigger)

                    if close_enabled and any(close_buy):

                        if self.mode == "live":
                            orderkwargs = dict(data=e,
                                               exectype=bt.Order.Limit,
                                               price=utils.ibPricing(e._name)["ask"],
                                               outsideRth=True,
                                               valid=bt.date2num(datetime.now() + timedelta(minutes=5))
                                               )
                        else:
                            orderkwargs = dict(data=e)

                        self.orders[e] = self.close(**orderkwargs)  # stop met - get out
                        self.current_action[d] = "Closing!!!"
                        #self.long_inds[d]['pstop'] = None
                    else:
                        self.current_action[d] = "Waiting to close"
                        if atr_in_strategy:
                            # Update only if smaller than
                            self.long_inds[d]['pstop'] = min(pstop, pclose + pdist)
                            if self.printLog and self.mode != 'optimize':
                                print("ATR Trail Stop Loss Short =", self.long_inds[d]['pstop'])
                            #return

                if self.mode == "live":
                    name = d._name.split("-")[0]
                    pstop = self.long_inds[d]['pstop']
                    value = {"pstop": pstop}

                    jsonfile.updateFile("contract", name, value)

            if self.mode != 'optimize':
                if (self.mode != "live") or (self.mode == "live" and self.datastatus[e]):

                    print_pstop = round(self.long_inds[d]['pstop'],2) if self.long_inds[d]['pstop'] is not None else ""
                    print(" Cash=", round(self.cash, 2), " Total Value=", round(self.value, 2), 'PStop', print_pstop, end="")

                    comminfo = self.broker.getcommissioninfo(e)

                    if pos != 0:
                        cur_pnl = comminfo.profitandloss(pos, e.close[-1], e.close[0])
                        cur_ret = self.short_inds[e]['pct'][0] * 100
                    else:
                        cur_pnl = 0
                        cur_ret = 0
                    print(" Pnl=", round(cur_pnl, 2), "Ret=", round(cur_ret, 4))

                    # Store pnl in SQLite table
                    dt_str = dt.strftime('%Y-%m-%d %H:%M:%S')
                    new_row_sql = (dt_str, self.strategy, e._name, pos, e.close[0], cur_ret, cur_pnl,)
                    self.cur.execute("INSERT INTO pnl VALUES (?, ?, ?, ?, ?, ?, ?)", new_row_sql)
                    self.con.commit()

                    # If mode is live save database to csv every cycle
                    if self.mode == 'live':
                        query = f"SELECT * FROM pnl WHERE Strategy = '{self.strategy}'"
                        self.pnl = pd.read_sql_query(query, self.con)
                        try:
                            self.pnl.to_csv(self.filenamepnl)
                        except PermissionError:
                            print("Permission error while saving. Please check for opened files")

                    if self.printLog:
                        print("Available cash=", self.cash, " Total portfolio value=", self.value, 'PStop',
                          self.long_inds[d]['pstop'])
                    if self.table:
                        table_content = {
                            "ver": config.params.version,
                            "round": str(self.round),
                            "mode": config.params.main.mode,
                            "contract": d._name.split('-')[0],
                            "strategy": self.strategy,
                            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            "stop_limit_hit": "Not implemented",
                            "buy_enabled": str(self.buy_authorized[d]),
                            "buy_order": "Not implemented",
                            "sell_enabled": str(self.sell_authorized[d]),
                            "sell_order": "Not implemented",
                            "current_action": str(self.current_action[d]),
                            "short_length": self.candleSize,
                            "short_timestamp": e.datetime.datetime(0).isoformat(),
                            "short_open": str(self.short_inds[e]["dataopen"][0]),
                            "short_high": str(self.short_inds[e]["datahigh"][0]),
                            "short_low": str(self.short_inds[e]["datalow"][0]),
                            "short_close": str(self.short_inds[e]["dataclose"][0]),
                            "position": str(pos),
                            "long_length": self.longCandleSize,
                            "long_timestamp": d.datetime.datetime(0).isoformat(),
                            "long_open": str(self.long_inds[d]["dataopen"][0]),
                            "long_high": str(self.long_inds[d]["datahigh"][0]),
                            "long_low": str(self.long_inds[d]["datalow"][0]),
                            "long_close": str(self.long_inds[d]["dataclose"][0]),
                            "gain": str(round(pos_gain, 2)) if pos_gain is not None else "",
                            "gain_expected": str(round(gain_expected, 2)) if gain_expected is not None else "",
                            "mfi": str(round(self.short_inds[e]["osc"][0], 2)) if osc_in_strategy and self.oscillator == "MFI" else "",
                            "rsi": str(round(self.short_inds[e]["osc"][0], 2)) if osc_in_strategy and self.oscillator == "RSI" else "",
                            "stoch": str(round(self.short_inds[e]["osc"][0], 2)) if osc_in_strategy and self.oscillator == "STOCH" else "",
                            "bb_top": str(round(self.long_inds[d]["bb"].top[0], 2)) if bb_in_strategy else "",
                            "bb_bot": str(round(self.long_inds[d]["bb"].bot[0], 2)) if bb_in_strategy else "",
                            "bb_pct": str(round(self.long_inds[d]["bb"].pctb[0], 2)) if bb_in_strategy else "",
                            "bb_buy": str(self.long_inds[d]['bblcross'][0]) if bb_in_strategy else "",
                            "bb_sell": str(self.long_inds[d]['bbscross'][0]) if bb_in_strategy else "",
                            "str": str(round(self.long_inds[d]['super'][0], 2)) if trend_in_strategy and self.trend == "STR" else "",
                            "plusdi": str(round(self.long_inds[d]["dmi"].plusDI[0], 2)) if trend_in_strategy and self.trend == "DMI" else "",
                            "minusdi": str(round(self.long_inds[d]["dmi"].minusDI[0], 2)) if trend_in_strategy and self.trend == "DMI" else "",
                            "adx": str(round(self.long_inds[d]["dmi"].adx[0],2)) if trend_in_strategy and self.trend == "DMI" else "",
                            "macd1": str(self.macd1),
                            "macd2": str(self.macd2),
                            "macdsig": str(self.macdsig),
                            "atrperiod": str(self.atrperiod),
                            "atrdist": str(self.atrdist),
                            "smaperiod": str(self.smaperiod),
                            "dirperiod": str(self.dirperiod),
                            "macd_value": str(round(self.long_inds[d]["macd"].macd[0], 2)) if macd_in_strategy else "",
                            "sig_value": str(round(self.long_inds[d]["macd"].signal[0], 2)) if macd_in_strategy else "",
                            "macd_cross": str(self.long_inds[d]["mcross"][0]) if macd_in_strategy else "",
                            "sma_value": str(round(self.long_inds[d]["sma"][0], 2)) if mean_return_close else "",
                            "sma_dir": str(round(self.long_inds[d]['smadir'][0], 2)) if trend_in_strategy and trend == "SMA" else "",
                            "atr": str(round(self.long_inds[d]['atr'][0], 2)) if atr_in_strategy else "",
                            "pstop": str(round(self.long_inds[d]['pstop'], 2)) if self.long_inds[d]['pstop'] is not None else "",
                            #"hl2": str(self.long_inds[d]["hl2"][0]),
                            #"percent_change":str(round(self.long_inds[d]['percentchange'][0],2)),
                            #"percentile_short":str(round(self.long_inds[d]['percentile_indicator'].short[0],2)),
                            #"percentile_long":str(round(self.long_inds[d]['percentile_indicator'].long[0],2)),
                            "total_value": str(round(self.value, 2)),
                            "total_cash": str(round(self.value-abs(self.short_inds[e]["dataclose"][0])*pos*self.multiplier[d], 2)),
                            "open_pos": str(round(self.short_inds[e]["dataclose"][0]*pos*self.multiplier[d],2)) if pos != 0 else "",
                            "buy_price": str(round(self.buyprice[e], 2)) if self.buyprice[e] is not None else "", #if pos != 0 else "",
                            "historic_pnl": str(self.total_pnl[e]),
                            "open_price": str(self.open_price[e]) if self.open_price[e] is not None else "",
                            "close_price": str(self.close_price[e]) if self.close_price[e] is not None else "",
                            "amount": str(self.trade_amount[e]) if self.trade_amount[e] is not None else "",
                            "last_pnl": str(self.last_pnl[e])
                        }
                        table_display(**table_content)
            if self.mode == "live":
                print("Available cash=", self.cash, " Total portfolio value=", self.value, 'PStop',
                      self.long_inds[d]['pstop'])

        if self.mode == "live":
            if self.datastatus[e]:
                self.datastatus[e] += 1
            jsonfile.updateFile("strategy", self.strategy, {"LASTCYCLE": datetime.now().strftime('%Y-%m-%d %H:%M:%S')})
            jsonfile.updateFile("strategy", self.strategy, {"CANDLES": self.candleSize})
            jsonfile.updateFile("strategy", self.strategy, {"LONGCANDLES": self.longCandleSize})

    def stop(self):

        if self.mode != 'optimize':
            print('')
            print('========================== STRATEGY BACKTEST FINISHED ==========================')
            print('')
            print("Strategy config parameters: ", self.strategy, config.config_data["strategies"][self.strategy])
            for cont in self.contractNames:
                print(cont, " Config parameters", config.config_data["contracts"][cont])

            # Retrieve tradelog from SQLite and save csv
            self.tradelog = pd.read_sql_query("SELECT * FROM tradelog", self.con)
            try:
                self.tradelog.to_csv(self.filenametxt, index=False)
            except PermissionError:
                print("Permission error while saving. Please check for opened files")

            # Retrieve pnl database from SQLite
            query = f"SELECT * FROM pnl WHERE Strategy = '{self.strategy}'"
            self.pnl = pd.read_sql_query(query, self.con)

            # Save pnl database to csv
            if self.mode == 'backtest':
                try:
                    self.pnl.to_csv(self.filenamepnl)
                except PermissionError:
                    print("Permission error while saving. Please check for opened files")

            # Calculate correlations from pnl database

            db_index = pd.DataFrame(self.pnl.Date)
            db_index['Date'] = pd.to_datetime(db_index['Date'])
            db_index.set_index("Date", inplace=True)
            db_index = db_index.loc[~db_index.index.duplicated(keep="first")]
            contracts = self.pnl.Contract.unique()

            for cont in contracts:
                df = self.pnl[(self.pnl['Contract'] == cont)].copy()
                df['Date'] = pd.to_datetime(df['Date'])
                df.set_index("Date", inplace=True)
                df = df[["PctChange"]]
                df.rename(columns={'PctChange': cont}, inplace=True)
                df = df.loc[~df.index.duplicated(keep="first")]
                db_index = pd.concat([db_index, df], axis=1, verify_integrity=True)

            print("*******************Correlation Matrix*******************")
            pd.set_option('display.max_rows', None)
            pd.set_option('display.max_columns', None)
            pd.set_option('display.width', None)
            pd.set_option('display.max_colwidth', None)
            print(round(db_index.corr(), 2))

            # Calculate and print kelly criterion per contract
            print("*******************Kelly Criterion*******************")
            contracts = self.tradelog.Contract.unique()
            for cont in contracts:
                df = self.tradelog[(self.tradelog['Contract'] == cont) & (self.tradelog['Type'] == "close")]

                strike_rate = (len(df[df["PnL"] > 0]) / len(df)) * 100 if len(df) > 0 else 0
                average_won = df[df["PnL"] > 0]["PnL"].mean() if not df[df["PnL"] > 0].empty else 0
                average_lost = df[df["PnL"] < 0]["PnL"].mean() if not df[df["PnL"] < 0].empty else 0

                if average_won == 0 or average_lost == 0:
                    print("Missing values to calculate Kelly Criterion")
                else:
                    kc = (((strike_rate / 100) - ((1 - (strike_rate / 100)) / (average_won / -average_lost))) * 100)
                    print(cont, end="")
                    print(f' Kelly Criterion: {round(kc,2)} % » Half Kelly Criterion: {round(kc/2,2)} %')

        else:
            #latestclose = [self.short_inds[e]['dataclose'][0] for i, d in enumerate(self.datas)]
            print(f'LowP:{self.lowpercentile} HighP:{self.highpercentile} PerPer:{self.percentile_period} '
                  f'macd1:{self.macd1} macd2:{self.macd2} macdsig:{self.macdsig} ATRPer:{self.atrperiod} '
                  f'atrdist:{self.atrdist} smaperiod:{self.smaperiod} dirperiod:{self.dirperiod} '
                  f'oscillator:{self.oscillator} ob: {self.osc_ob} os: {self.osc_os} '
                  f'ob_exit: {self.osc_ob_exit} os_exit: {self.osc_os_exit} '
                  f'FinalValue: {round(self.stats.broker.value[0], 2)} Candle {self.candleSize} '
                  f'LongCandle {self.longCandleSize} Trend {self.trend}'
                  )

# ----------------------STRATEGIES------------------------ # END
