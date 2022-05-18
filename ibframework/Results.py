import logging

import backtrader as bt
import pandas as pd
import empyrical as ep     # https://github.com/quantopian/empyrical
from datetime import datetime

import Analyzers
from Strategies import *
import Indicators
import utils

from Config import json_config

# parse config
config = json_config()

mpl_logger = logging.getLogger('matplotlib')
mpl_logger.setLevel(logging.WARNING)


def fin_funcs(returns_series, risk_free_rate=0):
    """
    Financial calculations taken from Quantopians Empirical Library.

    :param df: pd.Series containing daily returns calculated on a percentage change and also by log scale.
    :return: Dictionary of financial ratios both for percent change returns and log returns.
    """
    returns_pct = returns_series

    # Calculate each of the functions.
    annual_return_pct = ep.annual_return(
        returns_pct, period="daily", annualization=None
    )
    cumm_return_pct = ep.cum_returns(returns_pct, starting_value=0).iloc[-1]
    cagr_pct = ep.cagr(returns_pct, period="daily", annualization=None)
    sharpe_pct = ep.sharpe_ratio(
        returns_pct, risk_free=risk_free_rate, period="daily", annualization=None
    )
    annual_volatility_pct = ep.annual_volatility(
        returns_pct, period="daily", alpha=2.0, annualization=None
    )
    max_drawdown_pct = ep.max_drawdown(returns_pct)
    calmar_pct = ep.calmar_ratio(returns_pct, period="daily", annualization=None)
    sortino_pct = ep.sortino_ratio(
        returns_pct,
        required_return=0,
        period="daily",
        annualization=None,
        _downside_risk=None,
    )
    tail_ratio_pct = ep.tail_ratio(returns_pct)

    # Collect ratios into dictionary.
    financials = {
        "annual_return": annual_return_pct,
        "cumm_return": cumm_return_pct,
        "cagr": cagr_pct,
        "sharpe": sharpe_pct,
        "annual_volatility": annual_volatility_pct,
        "max_drawdown": max_drawdown_pct,
        "calmar": calmar_pct,
        "sortino": sortino_pct,
        "tail_ratio": tail_ratio_pct,
    }

    return financials


def results(strat, strategies, contract):
    opt_results = []

    separator = '-'

    try:
        for run in strategies:
            for strategy in run:

                try:
                    sharpieRatio = round(strategy.analyzers.sr.get_analysis()['sharperatio'], 2)
                except (TypeError, AttributeError):
                    sharpieRatio = "Not defined"
                try:
                    wonTotal = round((strategy.analyzers.ta.get_analysis().won.total /
                           strategy.analyzers.ta.get_analysis().total.closed) * 100, 2)
                    closedTotal = round(strategy.analyzers.ta.get_analysis().total.closed, 2)
                except (KeyError, AttributeError):
                    wonTotal = "Not defined"
                    closedTotal = "Not defined"
                try:
                    opt_headers = (vars(strategy.params).keys())
                    opt_values = (vars(strategy.params).values())
                except AttributeError:
                    opt_headers = dict()
                    opt_values = dict()
                opt_headers = list(opt_headers)
                opt_values = list(opt_values)
                opt_headers.extend(
                    ['SQN', 'Sharpe Ratio', 'Max Drawdown', 'Position', 'Cash', 'Strike Rate', 'Total Closed','Final Value'])
                opt_values.extend([round(strategy.analyzers.sqn.get_analysis().sqn, 2),
                                   #round(strategy.analyzers.sr.get_analysis()['sharperatio'], 2),
                                   sharpieRatio,
                                   round(strategy.stats.drawdown.maxdrawdown[0], 2),
                                   round(strategy.cash, 2),
                                   wonTotal,
                                   closedTotal])
                open_positions_total = 0
                open_positions_items = [round(thisitem, 2) for thisitem in
                                        list(strategy.analyzers.pos_val.get_analysis().values())[-1]]
                for openPosition in open_positions_items:
                    open_positions_total = open_positions_total + abs(openPosition)
                opt_values.insert(len(opt_values)-2, open_positions_items)#opt_values.insert(16, open_positions_items)
                opt_values.insert(len(opt_values)+1, round(strategy.cash + open_positions_total, 2))#opt_values.insert(19, round(strategy.cash + open_positions_total, 2))
                opt_results.append(opt_values)


    except:
        for strategy in strategies:
            opt_headers = (vars(strategy.params).keys())
            opt_headers = list(opt_headers)
            opt_headers.extend(['SQN', 'Sharpe Ratio', 'Max Drawdown', 'Positions', 'Cash', 'Strike Rate', 'Trades Closed', 'Final Value'])
            opt_values = (vars(strategy.params).values())
            opt_values = list(opt_values)
            opt_values.extend([round(strategy.analyzers.sqn.get_analysis().sqn, 2),
                               round(strategy.analyzers.sr.get_analysis()['sharperatio'], 2),
                               round(strategy.stats.drawdown.maxdrawdown[0], 2),
                               round(strategy.cash, 2),
                               round((strategy.analyzers.ta.get_analysis().won.total /
                                      strategy.analyzers.ta.get_analysis().total.closed) * 100, 2),
                               round(strategy.analyzers.ta.get_analysis().total.closed, 2)])
            open_positions_total = 0
            open_positions_items = [round(thisitem, 2) for thisitem in
                                    list(strategy.analyzers.pos_val.get_analysis().values())[-1]]
            for openPosition in open_positions_items:
                open_positions_total = open_positions_total + abs(openPosition)
            opt_values.insert(len(opt_values)-2, open_positions_items)#opt_values.insert(16, open_positions_items)
            opt_values.insert(len(opt_values)+1, round(strategy.cash + open_positions_total, 2))#opt_values.insert(19, round(strategy.cash + open_positions_total, 2))
            opt_results.append(opt_values)

    opt_df = pd.DataFrame(opt_results, columns=opt_headers)
    opt_df.sort_values(by=['Final Value'], inplace=True, ascending=False)
    opt_df = opt_df.reset_index(drop=True)
    try:
        del opt_df['parameters']
        del opt_df['printlog']
        del opt_df['multi']
        del opt_df['oneplot']
        del opt_df['opt_candle_size']
        del opt_df['opt_contract']


    except:
        pass

    mode = config.params.main.mode
    contracts = config.get_contract_names_by_strat(strat)
    candles = config.get_contract_parameter(contracts[0], "candle_size")
    longCandles = config.get_contract_parameter(contracts[0], "long_candle_size")
    label = candles + "_" + longCandles
    resultsDir = config.params.data.results_output_dir

    utils.check_path(resultsDir)

    if mode == 'optimize':
        opt_df.to_csv(resultsDir + contract + '_' + label + '_OPT_Results_' +
                      datetime.now().strftime("%H_%M_%S") + '.csv')
    else:
        opt_df.to_csv(resultsDir + separator.join(contracts) + '_' + label + '_BT_Results_' +
                      datetime.now().strftime("%H_%M_%S") + '.csv')

def fin_results(strategies):

        fin_results = {}

        # Calculate the financial functions for the strategy results
        # First, get market values for the algorithm, second item in list in dict from analyzer cash_market
        dict_mv = strategies[0].analyzers.getbyname("cash_market").get_analysis()

        # Create lists for values and keys for pd.Series, then create pd.Series with pct_change.
        v = [x[1] for x in dict_mv.values()]
        d = [x.date() for x in dict_mv.keys()]
        returns_series = pd.Series(v, index=d, name="Strategy")

        #returns_series = returns_series[int(args.rperiod):].pct_change()
        returns_series = returns_series[int(250):].pct_change()


        # Call fin_funcs and get dictionary back. Add to new dictionary tracking all the financials.
        fin_results["Strategy"] = fin_funcs(returns_series)

        # Get the OHLCV for each data.
        dict_ohlcv = strategies[0].analyzers.getbyname("ohlcv").get_analysis()

        # Create a dataframe from the analyzer ohlcv, percent change by date.
        df = (
            pd.DataFrame(pd.DataFrame.from_dict(dict_ohlcv).unstack())
                .loc[pd.IndexSlice[:, :, 3], :]
                .droplevel(2)
                .reset_index()
                .pivot(index="level_0", columns="level_1", values=0)
                .pct_change()
        )

        # Get financial ratios for each security column and add to fin_results dictionary.
        for n in range(len(df.columns)):
            fin_results[df.columns[n]] = fin_funcs(df.iloc[:, n])

        df_results = pd.DataFrame.from_dict(fin_results)

        return df_results

def printAnalyzers(cerebroResults, strategy, contract):

    mode = config.params.main.mode
    contracts = config.get_contract_names_by_strat(strategy)
    cash = config.get_strategy_parameter(strategy, "cash")

    firstStrat = cerebroResults[0]
    print('')

    if mode != 'optimize':
        print('Analyzed ', contracts, file=open("./logs/logfile.log", "a"))
        print('')
        logging.debug("Ta", firstStrat.analyzers.ta.get_analysis())
        logging.debug("Ta", firstStrat.analyzers.ta.get_analysis())
        Analyzers.printShortTA(firstStrat.analyzers.ta.get_analysis(),
                     firstStrat.analyzers.sqn.get_analysis(),
                     firstStrat.analyzers.sr.get_analysis(),
                     firstStrat.analyzers.pos_val.get_analysis(),
                     firstStrat.analyzers.drawdown.get_analysis(),
                     firstStrat.stats.broker,#list(firstStrat.stats.cash)[0],
                     cash)

        Analyzers.printTR(firstStrat.analyzers.tr.get_analysis())
    else:
        print('Optimized ', contract)
        print('')

    # print the analyzers

    # printTradeAnalysis(firstStrat.analyzers.ta.get_analysis())
    # printSQN(firstStrat.analyzers.sqn.get_analysis())
    # printSR(firstStrat.analyzers.sr.get_analysis())
    # printTR(firstStrat.analyzers.tr.get_analysis())