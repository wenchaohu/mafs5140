from collections import deque

import numpy as np
import pandas as pd

short_win_size = 30
long_win_size = 60
asset_names = ['btc', 'eth', 'ltc', 'xrp']
name_idx_map = dict((name, idx) for idx, name in enumerate(asset_names))

ret_stats = pd.DataFrame(
    data={'mean': [-1.54776558e-05, -8.50344245e-06, -1.72354371e-05,  3.49314544e-06],
          'stddev': [0.002126, 0.00161883, 0.00207338, 0.00112582]},
    index=['btc', 'eth', 'ltc', 'xrp']
)

short_z_score_thres = 2
long_z_score_thres = -2
flatten_long_z_score = 6
flatten_short_z_score = -6
target_position = 10000


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
    position_new = position_current
    if counter < 1:
        # # ring buffer pointers
        memory.short_rb_ptr = 0
        memory.long_rb_ptr = 0
        # allocate dataframe to store historical data
        memory.short_win_data = dict((name, _init_hist_data_df(short_win_size)) for name in asset_names)
        memory.long_win_data = dict((name, _init_hist_data_df(long_win_size)) for name in asset_names)
        position_new = position_current
        memory.prev_two_close_px = deque()
        memory.prev_two_close_px.append(data[:, 3])
        memory.nb_trades = 0
        # return position_new, memory
    elif counter < 2:
        memory.prev_two_close_px.append(data[:, 3])
        # return position_new, memory
    else:
        memory.prev_two_close_px.popleft()
        memory.prev_two_close_px.append(data[:, 3])

    if counter > long_win_size:
        for name in asset_names:
            idx = name_idx_map[name]
            long_ma = memory.long_win_data[name]['close'].mean()
            short_ma = memory.short_win_data[name]['close'].mean()
            target_shares = target_position / np.mean(data[idx][0:4])
            if short_ma > long_ma and position_current[idx] < target_shares:
                position_new[idx] = target_shares
            elif short_ma < long_ma and position_current[idx] > -target_shares:
                # position_new[idx] -= target_position / np.mean(data[idx][0:4])
                # position_new[idx] = max(position_new[idx], 0)
                position_new[idx] = -target_shares

    for idx, v in enumerate(asset_names):
        memory.short_win_data[v].iloc[memory.short_rb_ptr] = data[idx]
        memory.long_win_data[v].iloc[memory.long_rb_ptr] = data[idx]

    memory.long_rb_ptr = (memory.long_rb_ptr + 1) % long_win_size
    memory.short_rb_ptr = (memory.short_rb_ptr + 1) % short_win_size

    return position_new, memory


def finalize():
    print('Done.')
