from pathlib import Path

import numpy as np
import pandas as pd

data_path = Path(r'F:\mafs\mafs5140\raw_data')

raw_bar_data = {}
raw_bar_data['btc'] = pd.read_csv(data_path / 'preprocessed_BTCUSDT.csv')
raw_bar_data['eth'] = pd.read_csv(data_path / 'preprocessed_ETHUSDT.csv')
raw_bar_data['ltc'] = pd.read_csv(data_path / 'preprocessed_LTCUSDT.csv')
raw_bar_data['xrp'] = pd.read_csv(data_path / 'preprocessed_XRPUSDT.csv')

all_close_px = None
for name, df in raw_bar_data.items():
    df = df.set_index('time')
    df.index = pd.to_datetime(df.index, format='%Y-%m-%d %H:%M:%S')
    close_px = df[['close']]
    close_px.columns = [name]
    close_px = close_px.loc[~close_px.index.duplicated(keep='first')]
    all_close_px = pd.concat([all_close_px, close_px], join='inner', axis=1)


bar_ret = all_close_px / all_close_px.shift(1) - 1
bar_ret = bar_ret.loc['20190801':]
bar_ret['diff_btc_eth'] = bar_ret['btc'] - bar_ret['eth']
bar_ret['diff_btc_ltc'] = bar_ret['btc'] - bar_ret['ltc']
bar_ret['diff_btc_xrp'] = bar_ret['btc'] - bar_ret['xrp']
ret_diff_stats = {
    'mean': [bar_ret['diff_btc_eth'].mean(), bar_ret['diff_btc_ltc'].mean(), bar_ret['diff_btc_xrp'].mean()],
    'stddev': [bar_ret['diff_btc_eth'].std(), bar_ret['diff_btc_ltc'].std(), bar_ret['diff_btc_xrp'].std()]
}
ret_diff_stats = pd.DataFrame(data=ret_diff_stats, index=['diff_btc_eth', 'diff_btc_ltc', 'diff_btc_xrp'])
print('='*40)
print(ret_diff_stats.to_string())
