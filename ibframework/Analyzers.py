import logging

import backtrader as bt

from Config import json_config

# config logging
logging.basicConfig(level=logging.CRITICAL)
log = logging.getLogger('broadcast')

# parse config
config = json_config()


# ----------------------DEFINE ANALYZERS------------------------ # START



class CashMarket(bt.analyzers.Analyzer):
    """
    Analyzer returning cash and market values
    """

    def start(self):
        super(CashMarket, self).start()

    def create_analysis(self):
        self.rets = {}
        self.vals = 0.0

    def notify_cashvalue(self, cash, value):
        self.vals = (cash, value)
        self.rets[self.strategy.datetime.datetime()] = self.vals

    def get_analysis(self):
        return self.rets


class OHLCV(bt.analyzers.Analyzer):
    """This analyzer reports the OHLCV of each of datas.
    Params:
      - timeframe (default: ``None``)
        If ``None`` then the timeframe of the 1st data of the system will be
        used
      - compression (default: ``None``)
        Only used for sub-day timeframes to for example work on an hourly
        timeframe by specifying "TimeFrame.Minutes" and 60 as compression
        If ``None`` then the compression of the 1st data of the system will be
        used
    Methods:
      - get_analysis
        Returns a dictionary with returns as values and the datetime points for
        each return as keys
    """

    def start(self):
        tf = min(d._timeframe for d in self.datas)
        self._usedate = tf >= bt.TimeFrame.Days

    def next(self):
        # Cycle through the datas at each next and store the OHLCV
        # (d.l.lines.__len__())
        pvals = {}
        # Some datas I use have only 'Close' value used for signalling. The try statement
        # avoids errors on these datas.
        for d in self.datas:
            try:
                d.open[0]
                d.high[0]
                d.low[0]
                d.volume[0]
            except:
                continue
            else:
                pvals = [d.open[0], d.high[0], d.low[0], d.close[0], d.volume[0]]

                if self._usedate:
                    self.rets[(self.strategy.datetime.date(), d._name)] = pvals
                else:
                    self.rets[(self.strategy.datetime.datetime(), d._name)] = pvals

    def get_analysis(self):
        return self.rets



def printTradeAnalysis(analyzer):
    '''
    Function to print the Technical Analysis results in a nice format.
    '''
    # Get the results we are interested in
    total_open = analyzer.total.open
    total_closed = analyzer.total.closed
    total_won = analyzer.won.total
    total_lost = analyzer.lost.total
    average_won = analyzer.won.pnl.average
    average_lost = analyzer.lost.pnl.average
    win_streak = analyzer.streak.won.longest
    lose_streak = analyzer.streak.lost.longest
    pnl_net = round(analyzer.pnl.net.total, 2)
    strike_rate = round((total_won / total_closed) * 100, 2)
    # Designate the rows
    h1 = ['Total Open', 'Total Closed', 'Total Won', 'Total Lost',"Average Won","Average Lost"]
    h2 = ['Strike Rate', 'Win Streak', 'Losing Streak', 'PnL Net']
    r1 = [total_open, total_closed, total_won, total_lost,average_won,average_lost]
    r2 = [strike_rate, win_streak, lose_streak, pnl_net]
    # Check which set of headers is the longest.
    if len(h1) > len(h2):
        header_length = len(h1)
    else:
        header_length = len(h2)
    # Print the rows
    print_list = [h1, r1, h2, r2]
    row_format = "{:<15}" * (header_length + 1)
    print("Trade Analysis Results:")
    for row in print_list:
        print(row_format.format('', *row))

def printShortTA(analyzer1, analyzer2, analyzer3, analyzer4,analyzer5, broker, investment):

    total_closed = analyzer1.total.closed
    total_won = analyzer1.won.total
    average_won = analyzer1.won.pnl.average
    max_won = analyzer1.won.pnl.max
    average_lost = analyzer1.lost.pnl.average
    average_long = analyzer1.long.pnl.average
    average_short = analyzer1.short.pnl.average
    max_lost = analyzer1.lost.pnl.max
    average_len = analyzer1.len.average
    min_len = analyzer1.len.min
    max_len = analyzer1.len.max
    win_streak = analyzer1.streak.won.longest
    lose_streak = analyzer1.streak.lost.longest
    strike_rate = round((total_won / total_closed) * 100, 2)
    sqn = round(analyzer2.sqn, 2)
    try:
        sr = round((analyzer3['sharperatio']), 2)
    except TypeError:
        sr = analyzer3['sharperatio']
    dd = round((analyzer5['max']['drawdown']), 2)
    value = round(broker.value[0],2)
    open_positions_total = 0
    open_positions_items = [round(thisitem,2) for thisitem in list(analyzer4.values())[-1]]
    kelly_criterion = (((strike_rate/100) - ((1-(strike_rate/100))/(average_won/-average_lost))) * 100) if average_lost !=0 else 100
    for open in open_positions_items:
        open_positions_total = open_positions_total + abs(open)
    print(f'Total Value: {broker.cash[0] + open_positions_total}  »  Cash: {broker.cash[0]}  »  Open Positions: {open_positions_items}')
    print(f'Strike Rate: {strike_rate} ({total_won}/{total_closed})  »  Win Streak: {win_streak}  »  Lose Streak: {lose_streak}')
    print(f"Average Won: {round(average_won,2)}  »  Average Lost: {round(average_lost,2)} »  Max Won: {round(max_won,2)} »  Max Lost: {round(max_lost,2)}")
    print(f"Average Long: {round(average_long,2)}  »  Average Short: {round(average_short,2)}")
    print(f"Average Lenght: {round(average_len, 2)} Candles  »  Min Lenght: {round(min_len, 2)} Candles »  Max Lenght: {round(max_len, 2)} Candles")
    print(f'Kelly Criterion: {round(kelly_criterion,2)} % » Half Kelly Criterion: {round(kelly_criterion/2,2)} %')
    print(f'Net Gain: {value - investment}  '#    print(f'Net Gain: {cash + open_positions_total - investment}  '
          f'Equivalent to: {round(((broker.value[0] - investment)*100/investment),2)} %')
    print(f'SQN: {sqn}  »  SharpeRatio: {sr} » Max Drawdown: {dd} % ')
    print('')
    return sqn, sr, strike_rate

def printSQN(analyzer):
    sqn = round(analyzer.sqn, 2)
    print('SQN: {}'.format(sqn))

def printSR(analyzer):
    sr = round((analyzer['sharperatio']), 2)
    print('SHARPE RATIO: {}'.format(sr))

def printTR(analyzer):
    print('-- Time Return:')
    for k, v in analyzer.items():
        print('{}: {}'.format(k, round((v), 4)))
    print('------------------')


# ----------------------DEFINE ANALYZERS------------------------ # END