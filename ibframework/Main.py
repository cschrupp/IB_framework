
from Config import json_config

import logging
import subprocess
import os
import sys
import time

from ib_insync import *

import utils
import Acquire
import Cerebro
import Results
# parse config
config = json_config()

# Create Logging dir
logsDir = config.params.data.logs_output_dir
logFilename = config.params.data.log_filename
fullLogsFilename = logsDir + "/" + logFilename
#utils.check_path(logsDir)

# config logging
logging.basicConfig(level=logging.CRITICAL, filename=fullLogsFilename)
log = logging.getLogger('broadcast')


def watchdog_app():

    # Trading modes: "paper", "live"
    gateway_port = config.params.connection.gateway_port
    gateway_version = config.params.watchdog.gateway_version
    gateway_mode = config.params.watchdog.gateway_mode
    trading_mode = config.params.watchdog.trading_mode
    connect_timeout = config.params.watchdog.connect_timeout
    app_startup_time = config.params.watchdog.app_startup_time
    app_timeout = config.params.watchdog.app_timeout
    retry_delay = config.params.watchdog.retry_delay

    ibc = IBC(gateway_version, gateway=gateway_mode, tradingMode=trading_mode)

    ibi = IB()

    ibi.connectedEvent += callLiveD

    watchdog = Watchdog(controller=ibc,
                        ib=ibi,
                        port=gateway_port,
                        connectTimeout=connect_timeout,
                        appStartupTime=app_startup_time,
                        appTimeout=app_timeout,
                        retryDelay=retry_delay
                        )

    watchdog.start()

    ibi.run()


def callLiveD():

    strategyList = config.get_all_strategy_names()
    LiveDPath = os.getcwd()
    _isWindows = os.sys.platform == 'win32'
    lives = dict()
    for strat in strategyList:
        if _isWindows:
            lives[strat] = subprocess.Popen(['start', "python", "LiveD.py", strat], shell=True)
            print(strat, "Running - Pid: ", lives[strat].pid)
            time.sleep(5)
        else:
            lives[strat] = subprocess.Popen(["python.exe", f'{LiveDPath}/LiveD.py', strat])


    """
    strategyList = config.get_all_strategy_names()
    lives = dict()
    for strat in strategyList:
        lives[strat] = subprocess.Popen(['start', "python", "LiveD.py", strat], shell=True)
        print(strat, "Running - Pid: ", lives[strat].pid)
        time.sleep(5)
    """

def callCerebro(slicedDatadic, strategy, contract):
    # Initialize Cerebro
    cerebro = Cerebro.runCerebro(slicedDatadic, strategy)
    # Run as many cerebro iterations as strategies and get results
    cerebroResults = cerebro.backtest()

    # Print Analyzer results for each strategy
    Results.printAnalyzers(cerebroResults, strategy, contract)

    """
    # Get and  print financial results for each strategy
    df_results = Results.fin_results(cerebroResults)
    print(df_results.round(3))
    """
    # Get and save to file strategy results for each strategy
    Results.results(strategy, cerebroResults, contract)


def main():

    # Config parameters
    mode = config.params.main.mode
    strategyList = config.get_all_strategy_names()
    logging.debug("strategy list", strategyList)

    if mode == 'live':
        # Spawn one instance of backtrader live for each enabled strategy, in charge of persinstence:
        # checks live IB gateway, checks for correct cycling of instances. Sends error messages as
        # necessary, restart instances as necessary.

        lives = dict()
        for strat in strategyList:
            lives[strat] = subprocess.Popen(['start', "python", "LiveD.py", strat], shell=True)
            print(strat, "Running - Pid: ", lives[strat].pid)
            time.sleep(5)

    else:
        # Load data
        dataload = Acquire.acquireData()
        # Acquire data: Acquires data by creating or updating csv files for
        fullDatadic = dataload.get_dataframe()

        for strategy in strategyList:
            # Separate data by strategy according to mode
            stratDatadic = dict()
            stratContracts = config.get_contracts_by_strat(strategy)
            logging.debug("Strategy Contracts", stratContracts)
            startDate = config.get_strategy_parameter(strategy, "start_date")
            endDate = config.get_strategy_parameter(strategy, "end_date")

            for contract in stratContracts:
                stratDatadic[contract] = fullDatadic[contract]

                if mode == 'optimize':
                    # Clear data dictionary
                    stratDatadic.clear()
                    stratDatadic[contract] = fullDatadic[contract]
                    # Slice data in the backtest interval
                    slicedDatadic = utils.setInterval(stratDatadic, startDate, endDate)

                    # Run as many cerebro iterations as strategies and get results
                    callCerebro(slicedDatadic, strategy, contract)

                    logging.debug("Strategy", strategy)
                    logging.debug("SliceDatadic", slicedDatadic)

            if mode == 'backtest':
                # Slice data in the backtest interval
                slicedDatadic = utils.setInterval(stratDatadic, startDate, endDate)

                # Run as many cerebro iterations as strategies and get results
                callCerebro(slicedDatadic, strategy, contract)

                logging.debug("Strategy", strategy)
                logging.debug("SliceDatadic", slicedDatadic)


if __name__ == "__main__":
    if config.params.main.mode == "live":
        if config.params.watchdog.enabled:
            watchdog_app()
        else:
            main()
    else:
        main()

