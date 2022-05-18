import logging

import backtrader as bt
import backtrader.filters as btfilters

import Analyzers
from Strategies import *
import Indicators
import utils
import datetime

from Config import json_config

# parse config
config = json_config()

mpl_logger = logging.getLogger('matplotlib')
mpl_logger.setLevel(logging.WARNING)

TFRAME = {'1 month': bt.TimeFrame.Months,
               '1 week': bt.TimeFrame.Weeks,
               '1 day': bt.TimeFrame.Days,
               '8 hours': bt.TimeFrame.Minutes,
               '4 hours': bt.TimeFrame.Minutes,
               '3 hours': bt.TimeFrame.Minutes,
               "2 hours": bt.TimeFrame.Minutes,
               '1 hour': bt.TimeFrame.Minutes,
               '20 mins': bt.TimeFrame.Minutes,
               '30 mins': bt.TimeFrame.Minutes,
               '15 mins': bt.TimeFrame.Minutes,
               '10 mins': bt.TimeFrame.Minutes,
               '5 mins': bt.TimeFrame.Minutes,
               '3 mins': bt.TimeFrame.Minutes,
               '2 mins': bt.TimeFrame.Minutes,
               '1 min': bt.TimeFrame.Minutes
               }

COMP = {'1 month': 1, '1 week': 1, '1 day': 1, '8 hours': 480, '4 hours': 240,
         '3 hours': 180, "2 hours": 120, '1 hour': 60, '30 mins': 30, '20 mins': 20, '15 mins': 15,
         '10 mins': 10, '5 mins': 5, '3 mins': 3, '2 mins': 2, '1 min': 1}

class runCerebro:

    def __init__(self, slicedDatadic, strategy):

        # Load config parameters
        self.slicedDatadic = slicedDatadic
        self.strategy = strategy
        self.stratFunc = eval(config.get_strategy_parameter(strategy, "base_strategy"))
        self.stratParameters = config.config_data["strategies"][strategy]["backtest"]
        logging.debug("Strategy parameters", self.stratParameters)
        self.contracts = config.get_contract_names_by_strat(strategy)
        self.cash = config.get_strategy_parameter(strategy, "cash")
        self.mode = config.params.main.mode
        self.dualData = config.get_strategy_parameter(strategy, "dual_data")
        self.percentSizer = config.get_strategy_parameter(strategy, "percent_sizer")
        if self.mode == "optimize":
            self.optParameters = config.get_full_opt_strategy_parameters(strategy)
        self.candleSize = config.get_strategy_parameter(strategy, "candle_size")
        self.timeFrame = TFRAME[self.candleSize]
        self.compression = COMP[self.candleSize]
        self.longCandleSize = config.get_strategy_parameter(strategy, "long_candle_size")
        self.longTimeFrame = TFRAME[self.longCandleSize]
        self.longCompression = COMP[self.longCandleSize]
        self.startDate = config.get_strategy_parameter(strategy, "start_date")
        self.endDate = config.get_strategy_parameter(strategy, "end_date")
        # Initialize cerebro
        maxCpu = 8 if self.mode == "optimize" else 1
        stdStats = False if self.mode == "optimize" else True
        optReturn = False if self.mode == "optimize" else False
        exactBars = False if self.mode == "optimize" else False
        tradeHistory = False if self.mode == "optimize" else True
        self.cerebro = bt.Cerebro(optreturn=optReturn, maxcpus=maxCpu, stdstats=stdStats, tradehistory=tradeHistory, exactbars=exactBars)
        # Initialize timer
        self.timer = utils.timer()
        # Set cash parameter
        self.cerebro.broker.set_cash(self.cash)
        # Load analyzer switches
        self.analyzers = config.config_data["analyzers"]
        # Load observer switches
        self.observers = config.config_data["observers"]
        # Load plot results switch
        self.plotResults = config.params.display.plot_results
        # Load writer switch
        self.writer = config.params.display.writer
        # Set analyzers
        if self.analyzers["time_return"]:
            self.cerebro.addanalyzer(bt.analyzers.TimeReturn, timeframe=bt.TimeFrame.Years, _name="tr")
        if self.analyzers["trade"]:
            self.cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name="ta")
        if self.analyzers["sqn"]:
            self.cerebro.addanalyzer(bt.analyzers.SQN, _name="sqn")
        if self.analyzers["sharpe_ratio"]:
            self.cerebro.addanalyzer(bt.analyzers.SharpeRatio, timeframe=bt.TimeFrame.Years,
                                     riskfreerate=0.01, _name="sr")
        if self.analyzers["positions_value"]:
            self.cerebro.addanalyzer(bt.analyzers.PositionsValue, _name="pos_val")
        if self.analyzers["draw_down"]:
            self.cerebro.addanalyzer(bt.analyzers.DrawDown, _name="drawdown")
        if self.analyzers["cash_market"]:
            self.cerebro.addanalyzer(Analyzers.CashMarket, _name="cash_market")
        if self.analyzers["ohlcv"]:
            self.cerebro.addanalyzer(Analyzers.OHLCV, _name="ohlcv")
        if self.analyzers["pyfolio"]:
            self.cerebro.addanalyzer(bt.analyzers.PyFolio, _name='pyfolio')
        # Set observers
        if self.observers["draw_down"]:
            self.cerebro.addobserver(bt.observers.DrawDown)  # visualize the drawdown
        if self.observers["cash"]:
            self.cerebro.addobserver(bt.observers.Cash)  # visualize the account value
        if self.observers["broker"]:
            self.cerebro.addobserver(bt.observers.Broker)  # visualize the account value
        if self.observers["pnl"]:
            self.cerebro.addobserver(indicators.Pnl)  # visualize the account value
        # Set writer
        if self.writer:
            self.cerebro.addwriter(bt.WriterFile, csv=True, out='Test.csv', rounding=2)
        # Set strategy
        if self.mode == "backtest":
            self.cerebro.addstrategy(self.stratFunc, strat=strategy, **self.stratParameters)
        elif self.mode == "optimize":
            self.cerebro.optstrategy(self.stratFunc, strat=strategy, **self.optParameters)
        # Set Sizer
        self.cerebro.addsizer(Indicators.finalSizer, percentSizer=self.percentSizer)

    def backtest(self):

        # Start time counter
        self.timer.setStartTime()
        # Parse dates
        startYear, startMonth, startDay = utils.parseDates(self.startDate)
        endYear, endMonth, endDay = utils.parseDates(self.endDate)
        # Cycle through provided datas
        for contract in self.slicedDatadic.keys():
            for n, dataname in enumerate(self.slicedDatadic[contract]):
                datas = self.slicedDatadic[contract][dataname]
                multiplier = config.get_contract_parameter(contract, "multiplier")
                margin = config.get_contract_parameter(contract, "margin")
                automargin = config.get_contract_parameter(contract, "automargin")
                commission = config.get_contract_parameter(contract, "commission")
                leverage = config.get_contract_parameter(contract, "leverage")

                # If data enumerate is 0 or even
                # for single data the default data to load will be the shorter candles data
                if (n % 2) == 0:
                    timeFrame = self.timeFrame
                    compression = self.compression
                    plotEnabled = True
                else:
                    timeFrame = self.longTimeFrame
                    compression = self.longCompression
                    plotEnabled = False

                # Load ordered candles datas
                data = bt.feeds.PandasData(
                                            fromdate=datetime.datetime(startYear, startMonth, startDay),
                                            todate=datetime.datetime(endYear, endMonth, endDay),
                                            dataname=datas,
                                            timeframe=timeFrame,
                                            compression=compression)
                if timeFrame != bt.TimeFrame.Days:
                    if timeFrame != bt.TimeFrame.Weeks:
                        if timeFrame != bt.TimeFrame.Months:
                            data.addfilter(btfilters.SessionFiller)
                data1 = self.cerebro.adddata(data, name=dataname)
                # Select plot enabled for data
                data1.plotinfo.plot = plotEnabled

                # Set commission scheme for each candles data
                self.cerebro.broker.setcommission(commission=commission, margin=margin, mult=multiplier,
                                                  leverage=leverage, automargin=automargin, name=dataname)

        print('Running {} strategy on: '.format(self.strategy), " ".join(self.contracts))
        print('Starting Portfolio Value: %.2f' % self.cerebro.broker.getvalue())

        cerebroResults = self.cerebro.run(runonce=False)

        self.timer.setStopTime()
        self.timer.printElapsedTime()

        # Plot Analyzer results for each strategy
        if self.plotResults:
            self.cerebro.plot(style='candlestick')

        return cerebroResults
