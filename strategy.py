from collections import deque

import numpy as np
import pandas as pd

short_win_size = 20
long_win_size = 50
asset_names = ['BTC', 'ETH', 'LTC', 'XRP']

diff_stats = pd.DataFrame(
    data={'mean': [7.958049e-07, 3.717348e-06, 2.092393e-06], 'stddev': [0.000701, 0.000979, 0.000869]},
    index=['diff_btc_eth', 'diff_btc_ltc', 'diff_btc_xrp']
)

short_z_score_thres = 3
long_z_score_thres = -3
flatten_z_score = 0.0
txn_position_usd = 10000


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
        # memory.short_rb_ptr = 0
        # memory.long_rb_ptr = 0
        # # allocate dataframe to store historical data
        # memory.short_win_data = dict((name, _init_hist_data_df(short_win_size)) for name in asset_names)
        # memory.long_win_data = dict((name, _init_hist_data_df(long_win_size)) for name in asset_names)
        # position_new = position_current
        memory.prev_two_close_px = deque()
        memory.prev_two_close_px.append(data[:, 3])
        memory.nb_trades = 0
        return position_new, memory
    elif counter < 2:
        memory.prev_two_close_px.append(data[:, 3])
        return position_new, memory
    else:
        memory.prev_two_close_px.popleft()
        memory.prev_two_close_px.append(data[:, 3])

    # write the most recent historical data into ring buffer
    # for idx, v in enumerate(asset_names):
    #     memory.short_win_data[v].iloc[memory.short_rb_ptr] = data[idx]
    #     memory.long_win_data[v].iloc[memory.long_rb_ptr] = data[idx]
    # memory.short_rb_ptr = (memory.short_rb_ptr + 1) % short_win_size
    # memory.long_rb_ptr = (memory.long_rb_ptr + 1) % long_win_size
    # calculate return of the two previous bar
    bar_ret = memory.prev_two_close_px[1] / memory.prev_two_close_px[0] - 1
    diff_btc_eth = bar_ret[0] - bar_ret[1]
    diff_btc_ltc = bar_ret[0] - bar_ret[2]
    diff_btc_xrp = bar_ret[0] - bar_ret[3]
    z_score = {
        'btc_eth': (diff_btc_eth - diff_stats.loc['diff_btc_eth', 'mean']) / diff_stats.loc['diff_btc_eth', 'stddev'],
        'btc_ltc': (diff_btc_ltc - diff_stats.loc['diff_btc_ltc', 'mean']) / diff_stats.loc['diff_btc_ltc', 'stddev'],
        'btc_xrp': (diff_btc_xrp - diff_stats.loc['diff_btc_xrp', 'mean']) / diff_stats.loc['diff_btc_xrp', 'stddev'],
    }

    pair = 'btc_eth'
    pos_idx = 1
    # print(time[:11])
    if z_score[pair] > short_z_score_thres:
        if position_current[0] == 0 and position_current[pos_idx] == 0:
            position_new[0] = -txn_position_usd / data[0][3]
            position_new[pos_idx] = txn_position_usd / data[pos_idx][3]
            memory.nb_trades += 1
            # print(f'Short pair {pair}: {position_new[0]} vs {position_new[pos_idx]}')
    elif z_score[pair] < long_z_score_thres:
        if position_current[0] == 0 and position_current[pos_idx] == 0:
            position_new[0] = txn_position_usd / data[0][3]
            position_new[pos_idx] = -txn_position_usd / data[pos_idx][3]
            memory.nb_trades += 1
            # print(f'Long pair {pair}: {position_new[0]} vs {position_new[pos_idx]}')
    elif position_current[0] < 0 and position_current[pos_idx] > 0 and z_score[pair] <= flatten_z_score:
        position_new[0] = 0
        position_new[pos_idx] = 0
        memory.nb_trades += 1
        # print(f'Flatten: {position_new[0]} vs {position_new[pos_idx]}')
    elif position_current[0] > 0 and position_current[pos_idx] < 0 and z_score[pair] >= flatten_z_score:
        position_new[0] = 0
        position_new[pos_idx] = 0
        memory.nb_trades += 1
        # print(f'Flatten: {position_new[0]} vs {position_new[pos_idx]}')
    elif position_current[0] != 0 or position_current[pos_idx] != 0:
        # in case the position is not flatten fully due to liquidity limitation
        position_new[0] = 0
        position_new[pos_idx] = 0
        memory.nb_trades += 1

    # print(f'{counter} -> Flatten: {position_current[0]} vs {position_current[pos_idx]}')
    print(memory.nb_trades)
    return position_new, memory
