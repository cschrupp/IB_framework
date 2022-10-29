import asyncio
import configparser
import logging
import os
from contextlib import suppress
from dataclasses import dataclass
from typing import ClassVar, Union

from eventkit import Event

import ib_insync.util as util
from ib_insync.contract import Contract, Forex
from ib_insync.ib import IB

import asyncio
import logging
import math
import signal
import sys
import time
from dataclasses import fields, is_dataclass
from datetime import date, datetime, time as time_, timedelta, timezone
from typing import AsyncIterator, Awaitable, Callable, Iterator, List, Union
import psutil
from Json import JsonFiles

import eventkit as ev

jsonfile = JsonFiles()

globalErrorEvent = ev.Event()
"""
Event to emit global exceptions.
"""


def allowCtrlC():
    """Allow Control-C to end program."""
    signal.signal(signal.SIGINT, signal.SIG_DFL)


def logToFile(path, level=logging.INFO):
    """Create a log handler that logs to the given file."""
    logger = logging.getLogger()
    if logger.handlers:
        logging.getLogger('LiveD').setLevel(level)
    else:
        logger.setLevel(level)
    formatter = logging.Formatter(
        '%(asctime)s %(name)s %(levelname)s %(message)s')
    handler = logging.FileHandler(path)
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def logToConsole(level=logging.INFO):
    """Create a log handler that logs to the console."""
    logger = logging.getLogger()
    stdHandlers = [
        h for h in logger.handlers
        if type(h) is logging.StreamHandler and h.stream is sys.stderr]
    if stdHandlers:
        # if a standard stream handler already exists, use it and
        # set the log level for the ib_insync namespace only
        logging.getLogger('LiveD').setLevel(level)
    else:
        # else create a new handler
        logger.setLevel(level)
        formatter = logging.Formatter(
            '%(asctime)s %(name)s %(levelname)s %(message)s')
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        logger.addHandler(handler)

def dataclassAsDict(obj) -> dict:
    """
    Return dataclass values as ``dict``.
    This is a non-recursive variant of ``dataclasses.asdict``.
    """
    if not is_dataclass(obj):
        raise TypeError(f'Object {obj} is not a dataclass')
    return {field.name: getattr(obj, field.name) for field in fields(obj)}


def run(*awaitables: Awaitable, timeout: float = None):
    """
    By default run the event loop forever.

    When awaitables (like Tasks, Futures or coroutines) are given then
    run the event loop until each has completed and return their results.

    An optional timeout (in seconds) can be given that will raise
    asyncio.TimeoutError if the awaitables are not ready within the
    timeout period.
    """
    loop = asyncio.get_event_loop()
    if not awaitables:
        if loop.is_running():
            return
        loop.run_forever()
        result = None
        if sys.version_info >= (3, 7):
            all_tasks = asyncio.all_tasks(loop)  # type: ignore
        else:
            all_tasks = asyncio.Task.all_tasks()  # type: ignore
        if all_tasks:
            # cancel pending tasks
            f = asyncio.gather(*all_tasks)
            f.cancel()
            try:
                loop.run_until_complete(f)
            except asyncio.CancelledError:
                pass
    else:
        if len(awaitables) == 1:
            future = awaitables[0]
        else:
            future = asyncio.gather(*awaitables)
        if timeout:
            future = asyncio.wait_for(future, timeout)
        task = asyncio.ensure_future(future)

        def onError(_):
            task.cancel()

        globalErrorEvent.connect(onError)
        try:
            result = loop.run_until_complete(task)
        except asyncio.CancelledError as e:
            raise globalErrorEvent.value() or e
        finally:
            globalErrorEvent.disconnect(onError)

    return result


@dataclass
class LiveD:
    r"""
    Programmatic control over starting and stopping TWS/Gateway
    using IBC (https://github.com/IbcAlpha/IBC).

    Args:
        twsVersion (int): (required) The major version number for
            TWS or gateway.
        gateway (bool):
            * True = gateway
            * False = TWS
        tradingMode (str): 'live' or 'paper'.
        userid (str): IB account username. It is recommended to set the real
            username/password in a secured IBC config file.
        password (str): IB account password.
        twsPath (str): Path to the TWS installation folder.
            Defaults:

            * Linux:    ~/Jts
            * OS X:     ~/Applications
            * Windows:  C:\\Jts
        twsSettingsPath (str): Path to the TWS settings folder.
            Defaults:

            * Linux:     ~/Jts
            * OS X:      ~/Jts
            * Windows:   Not available
        ibcPath (str): Path to the IBC installation folder.
            Defaults:

            * Linux:     /opt/ibc
            * OS X:      /opt/ibc
            * Windows:   C:\\IBC
        ibcIni (str): Path to the IBC configuration file.
            Defaults:

            * Linux:     ~/ibc/config.ini
            * OS X:      ~/ibc/config.ini
            * Windows:   %%HOMEPATH%%\\Documents\IBC\\config.ini
        javaPath (str): Path to Java executable.
            Default is to use the Java VM included with TWS/gateway.
        fixuserid (str): FIX account user id (gateway only).
        fixpassword (str): FIX account password (gateway only).

    This is not intended to be run in a notebook.

    To use IBC on Windows, the proactor (or quamash) event loop
    must have been set:

    .. code-block:: python

        import asyncio
        asyncio.set_event_loop(asyncio.ProactorEventLoop())

    Example usage:

    .. code-block:: python

        ibc = IBC(976, gateway=True, tradingMode='live',
            userid='edemo', password='demouser')
        ibc.start()
        IB.run()
    """

    LiveDLogLevel: ClassVar = logging.DEBUG

    strategy: str = ''
    # LiveDPath: str = ''
    """
    twsVersion: int = 0
    gateway: bool = False
    tradingMode: str = ''
    twsPath: str = ''
    twsSettingsPath: str = ''
    ibcPath: str = ''
    ibcIni: str = ''
    javaPath: str = ''
    userid: str = ''
    password: str = ''
    fixuserid: str = ''
    fixpassword: str = ''
    """
    def __post_init__(self):
        self.LiveDPath = None
        self._isWindows = os.sys.platform == 'win32'
        if not self.LiveDPath:
            self.LiveDPath = os.getcwd()  # '/opt/ibc' if not self._isWindows else 'C:\\IBC'
            print("Path of the current directory : " + self.LiveDPath)
        self._proc = None
        self._monitor = None
        self._logger = logging.getLogger('LiveD.driver') # logging.getLogger('ib_insync.IBC')

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *_exc):
        print('exit method called')
        self.terminate()

    def start(self):
        """Launch TWS/IBG."""
        run(self.startAsync())

    def terminate(self):
        """Terminate TWS/IBG."""
        run(self.terminateAsync())

    async def startAsync(self):
        if self._proc:
            return
        self._logger.info('Starting')

        # map from field names to cmd arguments; key=(UnixArg, WindowsArg)
        args = dict(strategy=('', ''))
                    # LiveDPath=('--LiveD-path=', '/LiveDPath:'))

        """
        twsVersion=('', ''),
        gateway=('--gateway', '/Gateway'),
        tradingMode=('--mode=', '/Mode:'),
        twsPath=('--tws-path=', '/TwsPath:'),
        twsSettingsPath=('--tws-settings-path=', ''),
        ibcPath=('--ibc-path=', '/IbcPath:'),
        ibcIni=('--ibc-ini=', '/Config:'),
        javaPath=('--java-path=', '/JavaPath:'),
        userid=('--user=', '/User:'),
        password=('--pw=', '/PW:'),
        fixuserid=('--fix-user=', '/FIXUser:'),
        fixpassword=('--fix-pw=', '/FIXPW:'))
        """
        # create shell command
        # live = subprocess.Popen(['start', "python", "LiveD.py", strat], shell=True)
        cmd = ["python.exe",
               f'{self.LiveDPath}\\LiveD.py' if self._isWindows else
               f'{self.LiveDPath}/LiveD.py']
        for k, v in dataclassAsDict(self).items():
            arg = args[k][self._isWindows]
            if v:
                if arg.endswith('=') or arg.endswith(':'):
                    cmd.append(f'{arg}{v}')
                elif arg:
                    cmd.append(arg)
                else:
                    cmd.append(str(v))

        # run shell command
        print("Command=", cmd)
        #self._proc = await asyncio.create_subprocess_exec(
        # *cmd, stdout = asyncio.subprocess.PIPE)
        self._proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE) # "python Terminal_test.py Madcd_Strat_3"

        #self._proc = await asyncio.create_subprocess_shell(" ".join(cmd), stdout=asyncio.subprocess.PIPE)  # "python Terminal_test.py Madcd_Strat_3"
        print("proc=", self._proc)
        self._monitor = asyncio.ensure_future(self.monitorAsync())

    async def terminateAsync(self):
        print("In TerminateAsync", self._proc, self._monitor)
        if not self._proc:
            return
        self._logger.info('Terminating')
        if self._monitor:
            self._monitor.cancel()
            self._monitor = None
        if self._isWindows:
            import subprocess
            subprocess.call(
                ['taskkill', '/F', '/T', '/PID', str(self._proc.pid)])
        else:
            with suppress(ProcessLookupError):
                self._proc.terminate()
                await self._proc.wait()
        self._proc = None

    async def monitorAsync(self):
        while self._proc:
            line = await self._proc.stdout.readline()
            if not line:
                break
            self._logger.log(LiveD.LiveDLogLevel, line.strip().decode())

@dataclass
class Watchdog:
    """
    Start, connect and watch over the TWS or gateway app and try to keep it
    up and running. It is intended to be used in an event-driven
    application that properly initializes itself upon (re-)connect.

    It is not intended to be used in a notebook or in imperative-style code.
    Do not expect Watchdog to magically shield you from reality. Do not use
    Watchdog unless you understand what it does and doesn't do.

    Args:
        controller (Union[IBC, IBController]): (required) IBC or IBController
            instance.
        ib (IB): (required) IB instance to be used. Do no connect this
            instance as Watchdog takes care of that.
        host (str): Used for connecting IB instance.
        port (int):  Used for connecting IB instance.
        clientId (int):  Used for connecting IB instance.
        connectTimeout (float):  Used for connecting IB instance.
        readonly (bool): Used for connecting IB instance.
        appStartupTime (float): Time (in seconds) that the app is given
            to start up. Make sure that it is given ample time.
        appTimeout (float): Timeout (in seconds) for network traffic idle time.
        retryDelay (float): Time (in seconds) to restart app after a
            previous failure.
        probeContract (Contract): Contract to use for historical data
            probe requests (default is EURUSD).
        probeTimeout (float); Timeout (in seconds) for the probe request.

    The idea is to wait until there is no traffic coming from the app for
    a certain amount of time (the ``appTimeout`` parameter). This triggers
    a historical request to be placed just to see if the app is still alive
    and well. If yes, then continue, if no then restart the whole app
    and reconnect. Restarting will also occur directly on errors 1100 and 100.

    Example usage:

    .. code-block:: python

        def onConnected():
            print(ib.accountValues())

        ibc = IBC(974, gateway=True, tradingMode='paper')
        ib = IB()
        ib.connectedEvent += onConnected
        watchdog = Watchdog(ibc, ib, port=4002)
        watchdog.start()
        ib.run()

    Events:
        * ``startingEvent`` (watchdog: :class:`.Watchdog`)
        * ``startedEvent`` (watchdog: :class:`.Watchdog`)
        * ``stoppingEvent`` (watchdog: :class:`.Watchdog`)
        * ``stoppedEvent`` (watchdog: :class:`.Watchdog`)
        * ``softTimeoutEvent`` (watchdog: :class:`.Watchdog`)
        * ``hardTimeoutEvent`` (watchdog: :class:`.Watchdog`)
    """

    events = [
        'startingEvent', 'startedEvent', 'stoppingEvent', 'stoppedEvent',
        'softTimeoutEvent', 'hardTimeoutEvent', 'processNotFoundEvent']

    controller: LiveD #Union[IBC, IBController]
    processID: int = None
    strategy: str = None
    """
    ib: IB
    host: str = '127.0.0.1'
    port: int = 7497
    clientId: int = 2
    """
    connectTimeout: float = 2
    appStartupTime: float = 30
    appTimeout: float = 20
    retryDelay: float = 5
    readonly: bool = False
    # account: str = ''
    # probeContract: Contract = Forex('EURUSD')
    probeTimeout: float = 30

    def __post_init__(self):
        self.startingEvent = Event('startingEvent')
        self.startedEvent = Event('startedEvent')
        self.stoppingEvent = Event('stoppingEvent')
        self.stoppedEvent = Event('stoppedEvent')
        self.softTimeoutEvent = Event('softTimeoutEvent')
        self.hardTimeoutEvent = Event('hardTimeoutEvent')
        self.processNotFoundEvent = Event('processNotFoundEvent')
        if not self.controller:
            raise ValueError('No controller supplied')
        """
        if not self.ib:
            raise ValueError('No IB instance supplied')
        if self.ib.isConnected():
            raise ValueError('IB instance must not be connected')
        """
        self._runner = None
        self._logger = logging.getLogger('LiveD.Watchdog')

    def start(self):
        self._logger.info('Starting')
        self.startingEvent.emit(self)
        self._runner = asyncio.ensure_future(self.runAsync())

    def stop(self):
        self._logger.info('Stopping')
        self.stoppingEvent.emit(self)
        # self.ib.disconnect()
        self._runner = None

    async def runAsync(self):

        def onTimeout(idlePeriod):
            if not waiter.done():
                waiter.set_result(None)

        def onError(reqId, errorCode, errorString, contract):
            if errorCode in {100, 1100} and not waiter.done():
                waiter.set_exception(Warning(f'Error {errorCode}'))

        def onDisconnected():
            if not waiter.done():
                waiter.set_exception(Warning('Disconnected'))

        def getPID(strategy):
            value_dict, file_path = jsonfile.readFile("strategy", strategy) # Fix false when file not found
            print(value_dict)
            if value_dict:
                proc_id = value_dict["PID"]
                print(proc_id)
                return proc_id
            return False

        def findPID(pid):
            if pid:
                dict_pids = {
                    p.info["pid"]: p.info["name"]
                    for p in psutil.process_iter(attrs=["pid", "name"])
                }
                print(dict_pids)
                for p in dict_pids:
                    if p == pid:
                        print("found", dict_pids[p])
                        return True
            return False
        print("Runner First", self._runner)
        while self._runner:
            try:
                await self.controller.startAsync()
                await asyncio.sleep(self.appStartupTime)
                """
                await self.ib.connectAsync(
                    self.host, self.port, self.clientId, self.connectTimeout,
                    self.readonly, self.account)
                """
                self.startedEvent.emit(self)
                """
                self.ib.setTimeout(self.appTimeout)
                self.ib.timeoutEvent += onTimeout
                self.ib.errorEvent += onError
                self.ib.disconnectedEvent += onDisconnected
                """
                print("Runner Seccond", self._runner)
                while self._runner:
                    """
                    waiter = asyncio.Future()
                    print("awaiting 1")
                    await waiter
                    print("awaiting 2")
                    """
                    # soft timeout, probe the app with a historical request
                    self._logger.debug('Soft timeout')
                    self.softTimeoutEvent.emit(self)
                    """

                    probe = self.ib.reqHistoricalDataAsync(
                        self.probeContract, '', '30 S', '5 secs',
                        'MIDPOINT', False)
                    bars = None
                    with suppress(asyncio.TimeoutError):
                        bars = await asyncio.wait_for(probe, self.probeTimeout)
                    if not bars:
                        self.hardTimeoutEvent.emit(self)
                        raise Warning('Hard timeout')
                    self.ib.setTimeout(self.appTimeout)
                    """
                    result = findPID(getPID(self.strategy))
                    """
                    found = None
                    with suppress(asyncio.TimeoutError):
                        found = await asyncio.wait_for(result, self.probeTimeout)
                    """
                    if not result:
                        self.processNotFoundEvent.emit(self)
                        raise Warning('Process Not Found')
                    else:
                        await asyncio.sleep(self.probeTimeout)
            except ConnectionRefusedError:
                pass
            except Warning as w:
                self._logger.warning(w)
            except Exception as e:
                self._logger.exception(e)
            finally:
                """
                self.ib.timeoutEvent -= onTimeout
                self.ib.errorEvent -= onError
                self.ib.disconnectedEvent -= onDisconnected
                """
                await self.controller.terminateAsync()
                self.stoppedEvent.emit(self)
                if self._runner:
                    await asyncio.sleep(self.retryDelay)


if __name__ == '__main__':
    asyncio.get_event_loop().set_debug(True)
    logToConsole(logging.DEBUG)
    #ibc = IBC(976, gateway=True, tradingMode='paper')
#             userid='edemo', password='demouser')
    live = LiveD("Macd_Strat_1")
    #ib = IB()
    #allowCtrlC()
    app = Watchdog(live, strategy="Macd_Strat_1", appStartupTime=15, appTimeout=10) # Watchdog(live, ib, port=7497, appStartupTime=15, appTimeout=10)
    app.start()
    #IB.run()
    run()
    """
    try:
        run()
    except KeyboardInterrupt:
        LiveD().terminate()
    """