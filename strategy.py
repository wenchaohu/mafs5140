
import numpy as np
import pandas as pd

short_win_size = 20
long_win_size = 50
asset_names = ['BTC', 'ETH', 'LTC', 'XRP']


def _init_hist_data_df(win_size):
    cols = ["open", "high", "low", "close", "volume", "close time", "quote asset volume", "number of trades",
            "taker buy base asset volume", "taker buy quote asset volume"]
    df = pd.DataFrame(np.nan, index=np.arange(win_size), columns=cols)
    return df


def handle_bar(counter,  # a counter for number of minute bars that have already been tested
               time,  # current time in string format such as "2018-07-30 00:30:00"
               data,  # data for current minute bar (in format 2)
               init_cash,  # your initial cash, a constant
               transaction,  # transaction ratio, a constant
               cash_balance,  # your cash balance at current minute
               crypto_balance,  # your crpyto currency balance at current minute
               total_balance,  # your total balance at current minute
               position_current,  # your position for 4 crypto currencies at this minute
               memory  # a class, containing the information you saved so far
               ):

    if counter < 1:
        # ring buffer pointers
        memory.short_rb_ptr = 0
        memory.long_rb_ptr = 0
        # allocate dataframe to store historical data
        memory.short_win_data = dict((name, _init_hist_data_df(short_win_size)) for name in asset_names)
        memory.long_win_data = dict((name, _init_hist_data_df(long_win_size)) for name in asset_names)
        position_new = position_current
    elif counter > long_win_size:
        position_new = position_current
    else:
        position_new = position_current
    # write the most recent historical data into ring buffer
    for idx, v in enumerate(asset_names):
        memory.short_win_data[v].iloc[memory.short_rb_ptr] = data[idx]
        memory.long_win_data[v].iloc[memory.long_rb_ptr] = data[idx]
    memory.short_rb_ptr = (memory.short_rb_ptr + 1) % short_win_size
    memory.long_rb_ptr = (memory.long_rb_ptr + 1) % long_win_size
    return position_new, memory
