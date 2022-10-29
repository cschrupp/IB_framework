import logging

import backtrader as bt
import backtrader.filters as btfilters

import Analyzers
from Strategies import *
import Indicators
import utils
import datetime
import os

from Config import json_config

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

print(os.sys.platform)

TEST = ['1 month', '1 week', '1 day', '8 hours', '4 hours',
        '3 hours', "2 hours", '1 hour', '30 mins', '20 mins', '15 mins',
        '10 mins', '5 mins', '3 mins', '2 mins', '1 min']
for c in TEST:
    print(maxPeriodDays(strategy="Macd_Strat_3", candles=c))