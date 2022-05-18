import logging

import backtrader as bt
import scipy.stats
from backtrader.observers import MetaDataTrades

from Config import json_config
import utils


# config logging
logging.basicConfig(level=logging.CRITICAL)
log = logging.getLogger('broadcast')

# parse config
config = json_config()


class finalSizer(bt.Sizer):
    #Based on the value percent sizer, this sizer can handle both fixed stake and percent
    params = (
        ('percents', 20),
        ('percentSizer', False),
        ('retint', True),# return an int size or rather the float value
    )

    def __init__(self):
        pass

    def _getsizing(self, comminfo, cash, data, isbuy):
        name = data._name.split("-")[0]
        position = self.broker.getposition(data)
        accvalue = self.broker.getvalue()
        mode = config.params.main.mode
        contractSize = config.get_contract_parameter(name, "size")
        multiplier = config.get_contract_parameter(name, "multiplier")
        sectype = config.get_contract_parameter(name, "sectype")
        if mode == "live":
            margin = utils.ibPricing(data._name)["margin"]
        else:
            margin = config.get_contract_parameter(name, "margin")

        if not position:
            if self.p.percentSizer:
                if sectype == "FUT":
                    size = (accvalue * (contractSize / 100)) / margin
                else:
                    size = (accvalue * (contractSize / 100)) / (data.close[0] * multiplier)
                logging.debug("Percent Sizer:", self.p.percentSizer, "Contract Size:", contractSize)
            else:
                size = contractSize
        else:
            size = position.size

        if self.p.retint:
            size = int(size)

        return size


class PNL(bt.Indicator):

    lines = ('pnl',)
    params = (('period',1),)

    plotinfo = dict(subplot=True,
                    plotname='Profit and losses'
                    )

    def __init__(self):
        self.boughtPrice = self.broker.getposition(self.data)
        self.currentPrice = self.data.close
        print(self.boughtPrice)
        self.lines.pnl = ((self.data.high + self.data.low) / 2)

class SMAdir(bt.Indicator):


    lines = ('smadir',)
    params = (('period', 30), ('dirperiod', 10),)

    plotinfo = dict(subplot=True, plotname='SMA dir')

    def __init__(self):

        sma = bt.ind.SMA(self.data, period=self.p.period)
        self.lines.smadir = sma - sma(-self.p.dirperiod)

class ChandelierExit(bt.Indicator):
    ''' https://corporatefinanceinstitute.com/resources/knowledge/trading-investing/chandelier-exit/ '''

    lines = ('long', 'short')
    params = (('period', 22), ('multip', 3),)

    plotinfo = dict(subplot=False)

    def __init__(self):
        highest = bt.ind.Highest(self.data.high, period=self.p.period)
        lowest = bt.ind.Lowest(self.data.low, period=self.p.period)
        atr = self.p.multip * bt.ind.ATR(self.data, period=self.p.period)
        self.lines.long = highest - atr
        self.lines.short = lowest + atr

class HL2(bt.Indicator):


    lines = ('hl2',)
    params = (('period',1),)

    plotinfo = dict(subplot=False,
                    plotname='High Low mean'
                    )

    def __init__(self):
        self.lines.hl2 = ((self.data.high + self.data.low) / 2)



class StatPercentile(bt.Indicator):


    lines = ('short', 'long')
    params = (('lowpercentile', 5),('highpercentile', 95),("percentile_period", 300),('contract', None), ('candle_size','1 day'))

    plotinfo = dict(subplot=False,
                    plotname = 'T Statistic Pecentile',
                    plotvaluetags=False
                    )

    def _plotlabel(self):
        plabels = [self.p.lowpercentile*0.01]
        plabels += [self.p.highpercentile*0.01]
        return plabels

    def __init__(self):
        """
        # Calculate T parameters for percentile strategy
        df = utils.readCSV(self.p.contract, self.p.candle_size)
        print("Perc contract",self.p.contract,self.p.candle_size)
        # print("Candle Size",self.p.candle_size)

        dfhl2 = (df.high + df.low) / 2
        diffhl2 = (dfhl2.pct_change().dropna().astype(float))
        Tparameters = scipy.stats.t.fit(diffhl2)
        print("Tparameters",Tparameters)
        self.df = Tparameters[0]
        self.loc = Tparameters[1]
        self.scale = Tparameters[2]
        """
        # Calculate T parameters for percentile strategy
        df = utils.readCSV(self.p.contract, self.p.candle_size)
        df = df[-self.p.percentile_period:] if self.p.percentile_period != 0 else df
        print("Loading contract")
        #print("Perc contract",self.p.contract,self.p.candle_size)
        #print("Candle Size",self.p.candle_size)


        dfhl2 = (df.high + df.low) / 2
        diffhl2 = (dfhl2.pct_change().dropna().astype(float))
        Tparameters = scipy.stats.t.fit(diffhl2)
        print("Tparameters",Tparameters)
        df = Tparameters[0]
        loc = Tparameters[1]
        scale = Tparameters[2]

        hl2 = ((self.data.high + self.data.low) / 2)
        self.l.short = hl2 * (1 + scipy.stats.t(df=df, loc=loc, scale=scale).ppf(self.p.highpercentile*0.01))
        self.l.long = hl2 * (1 + scipy.stats.t(df=df, loc=loc, scale=scale).ppf(self.p.lowpercentile*0.01))
    """
    def next(self):
        print("Running next")
        hl2 = ((self.data.high + self.data.low) / 2)
        print("HL2", hl2)
        tdist = scipy.stats.t(df=self.df, loc=self.loc, scale=self.scale)
        print("tdist ppf",tdist.ppf(self.p.highpercentile * 0.01))

        self.l.short = hl2 * (1 + tdist.ppf(self.p.highpercentile * 0.01))
        self.l.long = hl2 * (1 + tdist.ppf(self.p.lowpercentile * 0.01))
    
    def once(self):
        print("Running once")
        # Calculate T parameters for percentile strategy
        df = utils.readCSV(self.p.contract, self.p.candle_size)
        print("Perc contract",self.p.contract,self.p.candle_size)
        # print("Candle Size",self.p.candle_size)

        dfhl2 = (df.high + df.low) / 2
        diffhl2 = (dfhl2.pct_change().dropna().astype(float))
        Tparameters = scipy.stats.t.fit(diffhl2)
        print("Tparameters",Tparameters)
        self.df = Tparameters[0]
        self.loc = Tparameters[1]
        self.scale = Tparameters[2]
    """

class MFI(bt.Indicator):
    lines = ('mfi',)
    params = dict(period=14)

    alias = ('MoneyFlowIndicator',)

    def __init__(self):
        tprice = (self.data.close + self.data.low + self.data.high) / 3.0
        mfraw = tprice * self.data.volume

        flowpos = bt.ind.SumN(mfraw * (tprice > tprice(-1)), period=self.p.period)
        flowneg = bt.ind.SumN(mfraw * (tprice < tprice(-1)), period=self.p.period)

        mfiratio = bt.ind.DivByZero(flowpos, flowneg, zero=100.0)
        self.l.mfi = 100.0 - 100.0 / (1.0 + mfiratio)

class Stochastic_Generic(bt.Indicator):
    '''
    This generic indicator doesn't assume the data feed has the components
    ``high``, ``low`` and ``close``. It needs three data sources passed to it,
    which whill considered in that order. (following the OHLC standard naming)
    '''
    lines = ('k', 'd', 'dslow',)
    params = dict(
        pk=14,
        pd=3,
        pdslow=3,
        movav=bt.ind.SMA,
        slowav=None,
    )

    def __init__(self):
        # Get highest from period k from 1st data
        highest = bt.ind.Highest(self.data0, period=self.p.pk)
        # Get lowest from period k from 2nd data
        lowest = bt.ind.Lowest(self.data1, period=self.p.pk)

        # Apply the formula to get raw K
        kraw = 100.0 * (self.data2 - lowest) / (highest - lowest)

        # The standard k in the indicator is a smoothed versin of K
        self.l.k = k = self.p.movav(kraw, period=self.p.pd)

        # Smooth k => d
        slowav = self.p.slowav or self.p.movav  # chose slowav
        self.l.d = slowav(k, period=self.p.pdslow)


class Pnl(bt.utils.py3.with_metaclass(MetaDataTrades, bt.observer.Observer)):
    _stclock = True

    params = (('usenames', True),)

    plotinfo = dict(plot=True, subplot=True, plothlines=[0.0],
                    plotymargin=0.10, plotlinelabels=True)

    plotlines = dict()

    def next(self):
        strat = self._owner
        for inst in strat.datas:
            pos = strat.broker.positions[inst]
            cur_line = getattr(self.lines, inst._name)
            comminfo = strat.broker.getcommissioninfo(inst)
            if len(self) == 1:
                cur_line[0] = 0
            else:
                if pos.size != 0:
                    cur_pnl = comminfo.profitandloss(pos.size, inst.close[-1],
                                                     inst.close[0])
                else:
                    cur_pnl = 0

                cur_line[0] = cur_line[-1] + cur_pnl