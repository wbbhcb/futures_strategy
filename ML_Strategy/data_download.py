import tushare as ts
import pandas as pd
import numpy as np
import datetime
from tqdm import tqdm
import os
from tqsdk import TqApi
import time
from tqsdk.tools import DataDownloader

import warnings
warnings.filterwarnings('ignore')

token = ''  # 需要自己注册一个tushare, 用自己的ts token
pro = ts.pro_api(token)
api = TqApi()
trade_date_min = datetime.date(2019, 1, 1)
trade_date_max = datetime.date(2020, 6, 10)


# 识别是否含有连续两个字
def myfun1(x):
    if '连续' in x:
        return False
    else:
        return True


# 提取字符串中的数字
def myfun2(x):
    num_list = [str(i) for i in range(10)]
    res = ''
    for tmp in x:
        if tmp in num_list:
            res += tmp
    return res


def get_zhuli():
    # 获取各个时段主力合约
    df_list = []
    for exchange in ['DCE', 'CZCE', 'SHFE', 'INE']:
        df = pro.fut_basic(exchange=exchange, fut_type='2', fields='ts_code,symbol,name')
        df['is_zhuli'] = df['name'].apply(myfun1)
        df = df[df['is_zhuli'] == True]
        df['exchange'] = exchange
        df_list.append(df[['ts_code', 'symbol', 'name', 'exchange']])
    df_list = pd.concat(df_list).reset_index(drop=True)

    df_zhuli = []
    for i in tqdm(range(len(df_list))):
        code = df_list['ts_code'][i]
        exchange = df_list['exchange'][i]
        symbol = df_list['symbol'][i]

        # 获取主力合约TF.CFX每日对应的月合约
        df = pro.fut_mapping(ts_code=code)
        df['trade_date'] = pd.to_datetime(df['trade_date'], infer_datetime_format=True)

        # 筛选到制定时间段
        df = df[(df['trade_date'].dt.date >= trade_date_min) & (df['trade_date'].dt.date <= trade_date_max)]

        # 获得成为主力合约的时间段
        df = df.groupby('mapping_ts_code')['trade_date'].agg({'max', 'min'}).reset_index()
        df['exchange'] = exchange
        df['symbol'] = symbol
        # df.columns = ['code', 'date_max', 'date_min', 'exchange', 'symbol']
        df = df.rename(columns={'mapping_ts_code': 'code', 'max': 'date_max', 'min': 'date_min'})
        df_zhuli.append(df)
    df_zhuli = pd.concat(df_zhuli).reset_index(drop=True)
    df_zhuli.to_csv('data/zhuli.csv', index=None)
    return df_zhuli


def get_1min(df_zhuli, day2idx, idx2day):
    # # 1分钟k线数据下载
    for i in tqdm(range(len(df_zhuli))):
        # if i >= 173:
        #     continue
        # break
        day1 = df_zhuli['date_max'][i]
        et = datetime.datetime(day1.year, day1.month, day1.day, 16)

        # 获取分时数据时，要从前一个交易日的21点开始
        day2 = df_zhuli['date_min'][i]
        st = day2.year*10000 + day2.month*100 + day2.day
        idx = day2idx[st]

        if idx == 0:
            st = datetime.datetime(st // 10000, st % 10000 // 100, st % 10000 % 100, 8)
        else:
            st = idx2day[idx-1]
            st = datetime.datetime(st//10000, st % 10000//100, st % 10000 % 100, 20)

        num = myfun2(df_zhuli['code'][i])

        symbol = df_zhuli['symbol'][i]
        exchange = df_zhuli['exchange'][i]

        #
        if exchange != 'CZCE':
            code = exchange + '.' + symbol.lower() + num
        else:
            code = exchange + '.' + symbol + num[1:]
        if code == 'CZCE.JR003': continue  # 这个文件有问题
        # print(code, st, et)
        save_path = os.path.join('data/1minute', code+".csv")

        # if code not in ['CZCE.JR009', 'SHFE.cu1902', 'SHFE.wr2005', 'SHFE.wr2101']:
        #     continue

        kd = DataDownloader(api, symbol_list=code, dur_sec=60,
                            start_dt=st, end_dt=et, csv_file_name=save_path)

        try:
            while not kd.is_finished():
                api.wait_update()
                # print("progress: kline: %.2f" % (kd.get_progress()))
                kd.get_progress()
        except Exception as e:
            print(code)
            print(e)
            # CZCE.JR009 SHFE.cu1902 SHFE.wr2005 SHFE.wr2101


def get_day(df_zhuli):
    for i in tqdm(range(len(df_zhuli))):
        # if i < 299: continue
        day1 = df_zhuli['date_max'][i]
        et = str(day1.year) + str(day1.month).zfill(2) + str(day1.day).zfill(2)
        day2 = df_zhuli['date_min'][i]
        st = str(day2.year) + str(day2.month).zfill(2) + str(day2.day).zfill(2)

        num = myfun2(df_zhuli['code'][i])

        symbol = df_zhuli['symbol'][i]
        exchange = df_zhuli['exchange'][i]

        #
        if exchange != 'CZCE':
            code = exchange + '.' + symbol.lower() + num
            save_path = os.path.join('data/day', code + ".csv")
        else:
            code = exchange + '.' + symbol + num[1:]
            save_path = os.path.join('data/day', code + ".csv")

        code = df_zhuli['code'][i]

        df = pro.fut_daily(ts_code=code, start_date=st, end_date=et)
        df.to_csv(save_path)
        time.sleep(0.3)


if __name__ == '__main__':
    # 创建文件夹
    if not os.path.exists('data'):
        os.mkdir('data')
    if not os.path.exists('data/1minute'):
        os.mkdir('data/1minute')
    if not os.path.exists('data/day'):
        os.mkdir('data/day')

    df_zhuli = get_zhuli()
    get_day(df_zhuli)

    # 获取哪些交易日
    df_day = []
    base_path = 'data'
    data_path = os.path.join(base_path, 'day')
    for path in tqdm(os.listdir(data_path)):
        df = pd.read_csv(os.path.join(data_path, path))
        df['ts_code'] = path[:-4]
        df_day.append(df)
    df_day = pd.concat(df_day)
    day_list = sorted(df_day['trade_date'].unique())
    day2idx = dict(zip(day_list, range(len(day_list))))
    idx2day = dict(zip(range(len(day_list)), day_list))

    get_1min(df_zhuli, day2idx, idx2day)

