import tushare as ts
import pandas as pd
import numpy as np
import datetime
from tqdm import tqdm
import os
import time
from config import config
import warnings


# 主力合约的一些信息获取
class ZhuLi_HeYue():

    def __init__(self, trade_date=None, now_time=None):
        self.zhuli = pd.DataFrame()
        self.zhuli_info = dict()
        self.pre_close_dict = dict()
        token = config.ts_token
        self.pro = ts.pro_api(token)
        self.pre_close_dict = dict()
        self.trade_date = trade_date
        self.now_time = now_time

    def get_zhuli(self):
        trade_date = self.trade_date

        def myfun(x, kind):
            numlist = [str(i) for i in range(10)]
            x = x.split('.')[0]
            name = ''
            number = ''
            for tmp in x:
                if tmp not in numlist:
                    name += tmp
                else:
                    number += tmp
            if kind == 1:
                return name
            else:
                return number

        def transform_name(symbol, number, exchange):
            # 将合约列表转化为天勤可识别的合约列表
            number = str(number)
            if exchange != 'CZCE':
                code = exchange + '.' + symbol.lower() + number
            else:
                code = exchange + '.' + symbol + number[1:]

            return code

        # 获取主力合约列表
        # 模拟回测时，trade_date要比实际trade_date小一天
        # 模拟交易则无所谓
        fields = 'ts_code,trade_date,pre_close,pre_settle,open,high,low,close,settle,vol,oi'
        df_list = []
        for exchange in ['DCE', 'CZCE', 'SHFE', 'INE']:
            if not trade_date:
                now = datetime.datetime.now()
                trade_date = str(now.year) + str(now.month).zfill(2) + str(now.day).zfill(2)
            df = self.pro.fut_daily(trade_date=trade_date, exchange=exchange, fields=fields)
            while len(df) == 0:
                # 如果没有，说明是在周末，往前平移一天
                df = self.pro.fut_daily(trade_date=trade_date, exchange=exchange, fields=fields)
                now = now - datetime.timedelta(days=1)
                trade_date = str(now.year) + str(now.month).zfill(2) + str(now.day).zfill(2)
            df['symbol'] = df['ts_code'].apply(lambda x: myfun(x, 1))
            df['number'] = df['ts_code'].apply(lambda x: myfun(x, 2))
            # 剔除持仓量小于1000的合约
            df = df[(df['number'] != '') & (df['oi'] > 1000)]
            df = df.sort_values(['symbol', 'oi'], ascending=[True, False]).reset_index(drop=True)
            df = df.drop_duplicates(subset=['symbol'], keep='first').reset_index(drop=True)
            df['exchange'] = exchange
            df_list.append(df)
        #     break
        df_list = pd.concat(df_list).reset_index(drop=True)
        # 将合约列表转化为天勤可识别的合约列表
        df_list['code'] = df_list.apply(lambda x: transform_name(x['symbol'], x['number'], x['exchange']), axis=1)
        self.zhuli = df_list
        return self.zhuli

    def get_info(self, trade_dates):
        # 把主力合约上一交易日的信息转化为字典保存起来方便调用
        zhuli_info = dict()
        for i in range(len(self.zhuli)):
            tmp_dict = dict()
            tmp_dict['open'] = self.zhuli['open'][i]
            tmp_dict['close'] = self.zhuli['close'][i]
            tmp_dict['high'] = self.zhuli['high'][i]
            tmp_dict['low'] = self.zhuli['low'][i]
            tmp_dict['vol'] = self.zhuli['vol'][i]
            zhuli_info[self.zhuli['code'][i]] = tmp_dict

        pre_close_dict = dict()
        day1 = str(self.zhuli['trade_date'][0])

        idx = trade_dates.index(day1)
        day2 = trade_dates[idx+1]

        for i in range(len(self.zhuli)):
            pre_close_dict[self.zhuli['code'][i]] = dict({day1: self.zhuli['pre_close'][i],
                                                       day2: self.zhuli['close'][i]})
        self.pre_close_dict = pre_close_dict
        return pre_close_dict

