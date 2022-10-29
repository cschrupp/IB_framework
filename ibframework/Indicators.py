import logging

import backtrader as bt
import scipy.stats
from backtrader.observers import MetaDataTrades
import numpy as np

from Config import json_config
import utils
from Json import JsonFiles

# config logging
logging.basicConfig(level=logging.CRITICAL)
log = logging.getLogger('broadcast')

# parse config
config = json_config()
# parse Json
jsonfile = JsonFiles()


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


class sofmaxSizer(bt.Sizer):
    #Based on the value percent sizer, this sizer can handle both fixed stake and percent
    params = (
        ('percents', 20),
        ('percentSizer', False),
        ('retint', True),# return an int size or rather the float value
    )

    def __init__(self):
        self.bt = {"XLP": 19.36, "SPY": 17.07, "XLV": 21.5, "XLU": 23.81, "FXI": 10.35, "QQQ": 22.62, "GLD": 17.98,
                       "XBI": 24.03, "USO": 22.13, "SLV": 17.48, "ITB": 23.83, "UNG": 13.37, "TSLA": 18.45,
                       "SMH": 21.03, "TLT": 20.27}

        values = list()
        for contract in self.bt:
            values.append(self.bt[contract])
        scalar_array = np.asarray(values) / np.sum(np.asarray(values))

        self.scalar = dict()
        for i, contract in enumerate(self.bt):
            self.scalar[contract] = round(scalar_array[i] * 100, 2)

        print(self.scalar)



        # pass

    def _getsizing(self, comminfo, cash, data, isbuy):
        cash_ratio = 1-(self.broker.getcash() / self.broker.getvalue())
        max_volume = data.volume[0] * 0.01
        name = data._name.split("-")[0]
        position = self.broker.getposition(data)
        accvalue = self.broker.getvalue() if cash_ratio < 0.4 else self.broker.getvalue() * 0.4
        mode = config.params.main.mode
        contractSize = self.scalar[name] if cash_ratio > 0.4 else self.bt[name]  # config.get_contract_parameter(name, "size")
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
                    if size > max_volume:
                        size = max_volume
                else:
                    size = (accvalue * (contractSize / 100)) / (data.close[0] * multiplier)
                    if size > max_volume:
                        size = max_volume
                logging.debug("Percent Sizer:", self.p.percentSizer, "Contract Size:", contractSize)
            else:
                size = contractSize
        else:
            size = position.size

        if self.p.retint:
            size = int(size)

        return size

class AverageTrueRangeTrailStop(bt.Indicator):
    '''
    Defined by J. Welles Wilder, Jr. in 1978 in his book *"New Concepts in
    Technical Trading Systems"*.

    The idea is to take the close into account to calculate the range if it
    yields a larger range than the daily range (High - Low)

    Formula:
      - SmoothedMovingAverage(TrueRange, period)

    See:
      - http://en.wikipedia.org/wiki/Average_true_range
    '''
    alias = ('ATRTS',)

    lines = ('atr', 'pdist', 'pstop',)
    params = (('atrperiod', 14), ('atrdist', 3), ("mode", None))
    plotinfo = dict(subplot=False)
    plotlines = dict(pstop_long=dict(color='blue', ls='--', _plotskip=False,),
                     pstop_short=dict(color='black', ls='-', _plotskip=False),
                     )

    def __init__(self):
        self.lines.atr = bt.indicators.AverageTrueRange(period=self.p.atrperiod)
        self.lines.pdist = self.lines.atr * self.p.atrdist
        self._tradeopen = list()
        self._longshort = False
        self._tradeclose = list()
        self._pstop_available = None
        self._previous_open_position = None
        self._previous_value = None
        self._live = None
        self._name = self.data._name.split("-")[0]
        if self.p.mode == "live":
            self._live = True

        super(AverageTrueRangeTrailStop, self).__init__()

    def sign_function(self, x):
        if x > 0:
            return 1
        elif x == 0:
            return 0
        else:
            return -1

    def tradeopen(self, data, size):
        self._tradeopen.append(data)
        self._longshort = self.sign_function(size)
        print("Data Size", self.data.size)

    def tradeclose(self, data):
        self._tradeopen.remove(data)
        self._tradeclose.append(data)

    def existing_position(self, pos):
        pass

    def datastatus(self, dataislive):
        pass
    def existing_pstop(self, data, value, size):
        self._previous_value = value
        self._previous_open_position = size
        print(self._previous_value, self._previous_open_position)
        if value and size:
            self._pstop_available = True
            self.tradeopen(data, size)
            print("Set for success")

    def next(self):

        if self._live:
            pass
            '''
            pos = bt.broker.BrokerBase.getposition(self, data=self.data).size
            if pos:
                if self.data in self._tradeopen:
                    pass
                else:
                    self._tradeopen.append(self.data)
            if not pos:
                if self.data in self._tradeclose:
                    pass
                else:
                    if self._longshort:
                        self._tradeclose.append(self.data)
            '''
        for d in self._tradeopen:
            if d == self.data:

                if self._pstop_available:
                    self.lines.pstop[0] = self._previous_value
                    self._pstop_available = False
                    print("Updating P-stop to value found in file:", self._previous_value)

                elif not self.lines.pstop[-1]:

                    pstop_long = self.data.close[0] - self.lines.pdist[0]
                    pstop_short = self.data.close[0] + self.lines.pdist[0]

                    if self._longshort == 1:
                        self.lines.pstop[0] = pstop_long
                    elif self._longshort == -1:
                        self.lines.pstop[0] = pstop_short

                else:

                    if self._longshort == 1:
                        self.lines.pstop[0] = max(self.data.close[0] - self.lines.pdist[0], self.lines.pstop[-1])
                    elif self._longshort == -1:
                        self.lines.pstop[0] = min(self.data.close[0] + self.lines.pdist[0], self.lines.pstop[-1])

                if self._live:
                    if self.lines.pstop[-1] != self.lines.pstop[0]:
                        pass
                        # jsonfile.updateFile("contract", self._name, {"pstop": self.lines.pstop[0]})
                        # print("P-stop updated in file")
                """
                print("Data=", self.data._name, "ATRTS Data Close", round(self.data.close[0], 2), "ATR",
                      round(self.lines.atr[0], 2), "ATR TS Pdist", round(self.lines.pdist[0], 2), "ATR TS Pstop",
                      round(self.lines.pstop[0], 2))
                """

        for d in self._tradeclose:
            if d == self.data:
                #print("ATR TS Reset due to trade closed")
                self.lines.pstop[0] = float("nan")
                self._tradeclose.remove(d)
                self._longshort = False

                if self._live:
                    jsonfile.updateFile("contract", self._name, {"pstop-1": None})
                    print("P-stop cleared in file")


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


class EWMACD(bt.Indicator):
    '''

    '''
    lines = ("std_dev", 'ewmacd', "ac_raw_ewmacd", "max", "min")
    #params = (('Lfast', [4, 8, 16, 32, 64]), ('Lslow', [16, 32, 64, 128, 256]), ('std_dev_period', 25),)
    params = (('macd1', [8, 16, 32, 64, 128, 256]), ('macd2', [16, 32, 64, 128, 256, 512]), ("macdsig", [4, 8, 16, 32, 64, 128]), ('std_dev_period', 25),)
    plotinfo = dict()
    plotlines = dict(ac_raw_ewmacd=dict(_plotskip=True), std_dev=dict(_plotskip=True))

    def ewmacd_forecast_scalar(self, macd1, macd2, macdsig):
        """
        Function to return the forecast scalar (table 49 of the book)

        Only defined for certain values
        """
        fsdict = dict(m4_8_2=100.6, m8_16_4=70.5, m16_32_8=50.3, m32_64_16=30.75, m64_128_32=20.65, m128_256_64=10.87, m256_512_128=5.5)
        #fsdict = dict(l2_8=0.53, l4_16=0.375, l8_32=0.265, l16_64=3.75, l32_128=2.65, l64_256=1.87)

        mkey = "m%d_%d_%d" % (macd1, macd2, macdsig)

        if mkey in fsdict:
            return fsdict[mkey]
        else:
            print(f"Warning: No scalar defined for macd1={macd1}, macd2={macd2}, macdsig{macdsig} using default of 1.0")
            return 1.0

    def ewmacd_forecast_weights(self, macd1, macd2, macdsig):
        """
        Function to return the forecast scalar (table 49 of the book)

        Only defined for certain values
        """

        #fwdict = dict(l2_8=0.42, l4_16=0.16, l8_32=0.42, l16_64=1, l32_128=1, l64_256=0.3)
        #fwdict = dict(l2_8=0.00, l4_16=0.05, l8_32=0.15, l16_64=0.2, l32_128=0.3, l64_256=0.3)
        #fwdict = dict(m4_8_2=0.01, m8_16_4=0.04, m16_32_8=0.15, m32_64_16=0.2, m64_128_32=0.3, m128_256_64=0.3)
        fwdict = dict(m4_8_2=0.0, m8_16_4=0.0, m16_32_8=0.0, m32_64_16=0.2, m64_128_32=0.2, m128_256_64=0.2, m256_512_128=0.2)
        mkey = "m%d_%d_%d" % (macd1, macd2, macdsig)

        if mkey in fwdict:
            return fwdict[mkey]
        else:
            print(f"Warning: No scalar defined for macd1={macd1}, macd2={macd2}, macdsig{macdsig} using default of 1.0")
            return 1.0

    def calc_ewmacd(self, std_dev, macd1, macd2, macdsig):
        """
        Calculate the ewmac trading fule forecast, given a price and EWMA speeds Lfast, Lslow and vol_lookback
        Assumes that 'price' and vol is daily data
        This version uses a precalculated price volatility, and does not do capping or scaling
        :param price: The price or other series to use (assumed Tx1)
        :type price: pd.Series
        :param vol: The daily price unit volatility (NOT % vol)
        :type vol: pd.Series aligned to price
        :param Lfast: Lookback for fast in days
        :type Lfast: int
        :param Lslow: Lookback for slow in days
        :type Lslow: int
        :returns: pd.DataFrame -- unscaled, uncapped forecast
        """
        # price: This is the stitched price series
        # We can't use the price of the contract we're trading, or the volatility will be jumpy
        # And we'll miss out on the rolldown. See
        # https://qoppac.blogspot.com/2015/05/systems-building-futures-rolling.html

        # We don't need to calculate the decay parameter, just use the span
        # directly

        ewmacd = bt.ind.MACD(self.data.close, period_me1=macd1, period_me2=macd2, period_signal=macdsig,
                             subplot=False,
                             plot=True)

        raw_ewmacd = ewmacd.macd - ewmacd.signal

        return bt.DivByZero(raw_ewmacd, std_dev)

    def __init__(self):
        full_ewmacd = list()
        #hl3 = (self.data.close + self.data.high + self.data.low)/3
        #returns = bt.ind.PctChange(self.data.close, period=self.p.std_dev_period) * 100
        daily_change = self.data.close(0) - self.data.close(-1)
        std_dev = bt.ind.StdDev(daily_change, period=self.p.std_dev_period)

        for i, (m1, m2, s) in enumerate(zip(self.p.macd1, self.p.macd2, self.p.macdsig)):
            f_scalar = self.ewmacd_forecast_scalar(m1, m2, s)
            f_weights = self.ewmacd_forecast_weights(m1, m2, s)
            full_ewmacd.append(self.calc_ewmacd(std_dev, m1, m2, s) * f_scalar * f_weights)

        raw_ewmacd = sum(full_ewmacd)

        self.lines.std_dev = std_dev
        self.lines.ewmacd = raw_ewmacd #* f_scalar #bt.DivByZero(raw_ewmac, std_dev) * f_scalar
        self.lines.ac_raw_ewmacd = bt.ind.Accum(abs(raw_ewmacd))
        self.lines.max = bt.ind.Max(raw_ewmacd)
        self.lines.min = bt.ind.Min(raw_ewmacd)


class EWMAC(bt.Indicator):
    '''

    '''
    lines = ("std_dev", 'ewmac', "ac_raw_ewmac", "max", "min")
    #params = (('Lfast', [4, 8, 16, 32, 64]), ('Lslow', [16, 32, 64, 128, 256]), ('std_dev_period', 25),)
    params = (('Lfast', [4, 8, 16, 32, 64, 128]), ('Lslow', [16, 32, 64, 128, 256, 512]), ('std_dev_period', 25),)
    plotinfo = dict()
    plotlines = dict(ac_raw_ewmac=dict(_plotskip=True))

    def ewmac_forecast_scalar(self, lfast, lslow):
        """
        Function to return the forecast scalar (table 49 of the book)

        Only defined for certain values
        """
        fsdict = dict(l2_8=10.6, l4_16=7.5, l8_32=5.3, l16_64=3.75, l32_128=2.65, l64_256=1.87, l128_512=0.94)
        #fsdict = dict(l2_8=0.53, l4_16=0.375, l8_32=0.265, l16_64=3.75, l32_128=2.65, l64_256=1.87)

        lkey = "l%d_%d" % (lfast, lslow)

        if lkey in fsdict:
            return fsdict[lkey]
        else:
            print(f"Warning: No scalar defined for Lfast={lfast}, Lslow={lslow} using default of 1.0")
            return 1.0

    def ewmac_forecast_weights(self, lfast, lslow):
        """
        Function to return the forecast scalar (table 49 of the book)

        Only defined for certain values
        """

        #fwdict = dict(l2_8=0.42, l4_16=0.16, l8_32=0.42, l16_64=1, l32_128=1, l64_256=0.3)
        #fwdict = dict(l2_8=0.00, l4_16=0.05, l8_32=0.15, l16_64=0.2, l32_128=0.3, l64_256=0.3)
        fwdict = dict(l2_8=0, l4_16=0.15, l8_32=0.15, l16_64=0.15, l32_128=0.25, l64_256=0.15, l128_512=0.15)
        lkey = "l%d_%d" % (lfast, lslow)

        if lkey in fwdict:
            return fwdict[lkey]
        else:
            print(f"Warning: No scalar defined for Lfast={lfast}, Lslow={lslow} using default of 1.0")
            return 1.0

    def calc_ewmac(self, std_dev, Lfast, Lslow):
        """
        Calculate the ewmac trading fule forecast, given a price and EWMA speeds Lfast, Lslow and vol_lookback
        Assumes that 'price' and vol is daily data
        This version uses a precalculated price volatility, and does not do capping or scaling
        :param price: The price or other series to use (assumed Tx1)
        :type price: pd.Series
        :param vol: The daily price unit volatility (NOT % vol)
        :type vol: pd.Series aligned to price
        :param Lfast: Lookback for fast in days
        :type Lfast: int
        :param Lslow: Lookback for slow in days
        :type Lslow: int
        :returns: pd.DataFrame -- unscaled, uncapped forecast
        """
        # price: This is the stitched price series
        # We can't use the price of the contract we're trading, or the volatility will be jumpy
        # And we'll miss out on the rolldown. See
        # https://qoppac.blogspot.com/2015/05/systems-building-futures-rolling.html

        # We don't need to calculate the decay parameter, just use the span
        # directly
        fast_ewma = bt.ind.ExponentialMovingAverage(self.data.close, period=Lfast)
        slow_ewma = bt.ind.ExponentialMovingAverage(self.data.close, period=Lslow)
        raw_ewmac = fast_ewma - slow_ewma

        return bt.DivByZero(raw_ewmac, std_dev)

    def __init__(self):
        full_ewmac = list()
        #hl3 = (self.data.close + self.data.high + self.data.low)/3
        #returns = bt.ind.PctChange(self.data.close, period=self.p.std_dev_period) * 100
        daily_change = self.data.close(0) - self.data.close(-1)
        std_dev = bt.ind.StdDev(daily_change, period=self.p.std_dev_period)


        for i, (f, s) in enumerate(zip(self.p.Lfast, self.p.Lslow)):
            f_scalar = self.ewmac_forecast_scalar(f, s)
            f_weights = self.ewmac_forecast_weights(f, s)
            full_ewmac.append(self.calc_ewmac(std_dev, f, s) * f_scalar * f_weights)

        raw_ewmac = sum(full_ewmac)

        self.lines.std_dev = std_dev
        self.lines.ewmac = raw_ewmac #* f_scalar #bt.DivByZero(raw_ewmac, std_dev) * f_scalar
        self.lines.ac_raw_ewmac = bt.ind.Accum(abs(raw_ewmac))
        self.lines.max = bt.ind.Max(raw_ewmac)
        self.lines.min = bt.ind.Min(raw_ewmac)

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