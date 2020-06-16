import pandas as pd
import numpy as np
import datetime
from tqdm import tqdm
import os

import warnings
warnings.filterwarnings('ignore')

base_path = 'data'
df_zhuli = pd.read_csv(os.path.join(base_path, 'zhuli.csv'))


def main():
    ##############
    # 读取分时数据
    ##############
    data_path = os.path.join(base_path, '1minute')
    df_min = []
    for path in tqdm(os.listdir(data_path)):
        try:
            df = pd.read_csv(os.path.join(data_path, path))
            df.columns = ['datetime', 'open', 'high', 'low', 'close', 'volume', 'open_oi', 'close_oi']
            df['ts_code'] = path[:-4]
            df_min.append(df)
        except:
            print('wrong file: %s' % path)

    df_min = pd.concat(df_min)
    df_min = df_min.reset_index(drop=True)
    df_min['datetime'] = pd.to_datetime(df_min['datetime'], infer_datetime_format=True)

    ##############
    # 读取日线数据
    ##############
    df_day = []
    data_path = os.path.join(base_path, 'day')
    for path in tqdm(os.listdir(data_path)):
        df = pd.read_csv(os.path.join(data_path, path))
        df['ts_code'] = path[:-4]
        df_day.append(df)
    df_day = pd.concat(df_day)

    # 有些合约一出来就是主力合约，没有pre_close，用open替换
    idx = df_day['pre_close'].isna()
    df_day.loc[idx, 'pre_close'] = df_day.loc[idx, 'open']
    df_day = df_day.reset_index(drop=True)

    ############
    # 交易日处理
    ############

    # 所有的交易日
    day_list = sorted(df_day['trade_date'].unique())
    day2idx = dict(zip(day_list, range(len(day_list))))
    idx2day = dict(zip(range(len(day_list)), day_list))
    day_set = set(day_list)

    # 获取所属交易日， 在21:00至00:00属于下个交易日
    def get_day(x):
        # 如果当天大于20点，算新的一天
        day = x.year * 10000 + x.month * 100 + x.day
        if x.hour > 20:
            idx = day2idx[day]
            day = idx2day[idx + 1]

        # 在凌晨的时候，周六凌晨也会交易
        if x.hour < 8:
            if day not in day_set:
                # 说明是在非工作日凌晨交易
                x = x - pd.Timedelta(days=1)
                day = x.year * 10000 + x.month * 100 + x.day
                idx = day2idx[day]
                day = idx2day[idx + 1]

        return day

    ################
    # 获取指数涨跌幅
    ################

    df_min['trade_date'] = df_min['datetime'].apply(get_day)
    df_min = df_min.merge(df_day[['ts_code', 'trade_date', 'pre_close']], on=['ts_code', 'trade_date'], how='left')
    # 剔除无昨日开盘价数据，这类数据几乎整天无交易
    df_min = df_min[~df_min['pre_close'].isna()].reset_index(drop=True)
    df_min['mean'] = (df_min['high'] + df_min['low'] + df_min['close'] + df_min['open']) / 4
    df_min['rate'] = (df_min['mean'] - df_min['pre_close']) / df_min['pre_close']
    df_min['second'] = df_min['datetime'].dt.second
    assert df_min['second'].max() == 0, '有问题'

    def get_cumvol(x):
        x = x.sort_values('datetime', ascending=True).reset_index(drop=True)
        x['cumvol'] = x['volume'].cumsum()
        return x[['datetime', 'cumvol']]

    tmp_df = df_min.groupby(['trade_date', 'ts_code']).apply(get_cumvol).reset_index()
    df_min = df_min.merge(tmp_df[['datetime', 'ts_code', 'cumvol']], on=['datetime', 'ts_code'], how='left')

    def get_index(x):
        index_rate = np.sum(x['rate'] * x['cumvol']) / np.sum(x['cumvol'])
        return index_rate

    index = df_min.groupby('datetime').apply(get_index).reset_index()
    index.columns = ['datetime', 'rate']

    return index


if __name__ == '__main__':
    index = main()
    index.to_csv(os.path.join(base_path, 'index.csv'), index=None)