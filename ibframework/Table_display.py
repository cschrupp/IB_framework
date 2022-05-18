from rich.console import Console
from rich.table import Table
from datetime import datetime



def table_display(ver,round,mode,contract,strategy,timestamp,
                  stop_limit_hit, buy_enabled, buy_order, sell_enabled,
                  sell_order, current_action, short_length, short_timestamp, short_open,
                  short_high, short_low, short_close,position, long_length, long_timestamp, long_open,
                  long_high, long_low, long_close, macd1, macd2 ,macdsig, atrperiod, atrdist, smaperiod, dirperiod,
                  #atrdist_trail,high_percentile,low_percentile,
                  macd_value, sig_value,macd_cross, sma_value, sma_dir, atr, pstop,
                  gain,
                  #hl2, percent_change, percentile_short, percentile_long,
                  total_value,total_cash,open_pos,
                  buy_price, historic_pnl,open_price,close_price,amount,last_pnl
                  ):


    table = Table(title="IB Trading Framework",show_lines=True)


    table.add_column("IB Trading Framework", justify="center", no_wrap=True)
    table.add_column("Version")
    table.add_column("Round",)
    table.add_column("Mode")
    table.add_column("Contract")
    table.add_column("Strategy")
    table.add_column("Timestamp")

    table.add_row("",ver,round,mode,contract,strategy,timestamp)
    table.add_row("Core checks", "Stop limit hit", "Buy enabled","Sending buy order", "Sell enabled","Sending sell order","Current action", style="bold")
    table.add_row("",stop_limit_hit,buy_enabled,buy_order,sell_enabled,sell_order,current_action)
    table.add_row("Contract short","Candles length","Candle timestamp","Open", "High", "Low","Close","Position",style="bold")
    table.add_row("",short_length, short_timestamp, short_open, short_high, short_low, short_close, position)
    table.add_row("Contract long","Candles length","Candle timestamp","Open", "High", "Low","Close","Position",style="bold")
    table.add_row("",long_length, long_timestamp, long_open, long_high, long_low, long_close, position)
    table.add_row("Parameters","MACD 1", "MACD 2", "MACD sig","ATR period","ATR dist","SMA period","SMA dir period",style="bold")
    table.add_row("",macd1,macd2,macdsig,atrperiod,atrdist,smaperiod,dirperiod)
    #table.add_row("","ATR dist trail", "High percentile", "Low percentile",style="bold")
    #table.add_row("",atrdist_trail,high_percentile,low_percentile)
    table.add_row("Indicators","MACD", "MACD sig","MACD cross","SMA","SMA direction", "ATR","P-STOP",style="bold")
    table.add_row("",macd_value,sig_value,macd_cross,sma_value, sma_dir, atr, pstop)
    table.add_row("Strategies", "Gain", style="bold")
    table.add_row("", gain)
    #table.add_row("","HL2", "Percent change","Percentile short","Percentile long",style="bold")
    #table.add_row("",hl2, percent_change, percentile_short, percentile_long)
    table.add_row("Account", "Total account value", "Available cash","Open position","Buy price","Historical PnL", style="bold")
    table.add_row("",total_value, total_cash,open_pos,buy_price,historic_pnl)
    table.add_row("Last transaction", "Open price", "Close price","Amount","PnL", style="bold")
    table.add_row("",open_price,close_price,amount,last_pnl)

    console = Console()
    console.print(table)

def main():
    table_display()

if __name__ == "__main__":
    main()