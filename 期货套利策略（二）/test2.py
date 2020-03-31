# -*- coding: utf-8 -*-
"""
Created on Mon Mar 23 19:33:33 2020

@author: hcb
"""

__author__ = 'wbbhcb'

from tqsdk import TqApi
from datetime import date
import datetime
from tqsdk import TqApi, TqBacktest, TargetPosTask, TqSim
import time
import statsmodels.tsa.stattools as ts
from statsmodels.tsa.stattools import coint
import numpy as np
import pandas as pd


# 协整检验的函数
def cointegration_test(series01, series02):
    p1 = ts.adfuller(series01, 1)[1]
    p2 = ts.adfuller(series01, 1)[1]
    # 同时平稳或不平稳则差分再次检验
    if (p1 > 0.05 and p2 > 0.05) or (p1 < 0.05 and p2 < 0.05):
        p1_diff = ts.adfuller(np.diff(series01), 1)[1]
        p2_diff = ts.adfuller(np.diff(series02), 1)[1]
        # 同时差分平稳进行OLS回归的残差平稳检验
        if p1_diff < 0.05 and p2_diff < 0.05:

            if coint(series01, series02)[1] > 0.05:
                result = False
            else:
                result = True
            return result
        else:
            return False
    else:
        return False


def get_param(klines1, klines2):
    diff = klines1['close'] - klines2['close']
    up = np.percentile(diff, 85)
    up_limit = np.percentile(diff, 99)
    # up_limit = np.max(diff)
    up_limit2 = np.percentile(diff, 60)

    # if up_limit<0:
    #     up_limit = up_limit * 0.8
    # else:
    #     up_limit = up_limit * 1.2

    down = np.percentile(diff, 15)

    down_limit = np.percentile(diff, 1)
    # down_limit = np.min(diff)
    # if down_limit<0:
    #     down_limit = down_limit * 1.2
    # else:
    #     down_limit = down_limit * 0.8

    down_limit2 = np.percentile(diff, 40)
    return up, up_limit, up_limit2, down, down_limit, down_limit2


acc = TqSim(init_balance=1000000)

# 在创建 api 实例时传入 TqBacktest 就会进入回测模式
api = TqApi(acc,  backtest=TqBacktest(start_dt=date(2020, 1, 1), end_dt=date(2020, 3, 1)),
            web_gui='http://127.0.0.1:8889')  #

kind1 = 'SHFE.cu2003'
kind2 = 'SHFE.cu2004'

# kind1 = 'SHFE.zn2005'
# kind2 = 'SHFE.zn2006'

# 获得 m1901 5分钟K线的引用
klines1 = api.get_kline_serial([kind1], 1 * 30, data_length=2000)
klines2 = api.get_kline_serial([kind2], 1 * 30, data_length=2000)
quote1 = api.get_quote(kind1)
quote2 = api.get_quote(kind2)
# # 创建 m1901 的目标持仓 task，该 task 负责调整 m1901 的仓位到指定的目标仓位
target_pos1 = TargetPosTask(api, kind1)
target_pos2 = TargetPosTask(api, kind2)

now1 = datetime.datetime.strptime(quote1.datetime, "%Y-%m-%d %H:%M:%S.%f")  # 当前quote的时间
now2 = datetime.datetime.strptime(quote2.datetime, "%Y-%m-%d %H:%M:%S.%f")  # 当前quote的时间
# api.wait_update()
# time1 = time.ctime(klines2.iloc[-1].datetime/(10**9))

while True:
    api.wait_update()
    if not pd.isna(klines2['close'][0]):
        break

flag = cointegration_test(np.array(klines1['close']), np.array(klines2['close']))
if flag:
    print('协整验证通过')
else:
    print('协整验证未通过')
#
up, up_limit, up_limit2, down, down_limit, down_limit2 = get_param(klines1, klines2)
print('up:%.3f, up_limit:%.3f, up_limit2:%.3f, down:%.3f, down_limit:%.3f, down_limit2:%.3f' %
      (up, up_limit, up_limit2, down, down_limit, down_limit2))
# up = up + 20
# up_limit = up_limit + 40
# down_limit = down_limit - 30
# down = down - 20
#

now_day = now1.day

while True:
    api.wait_update()
    now1 = datetime.datetime.strptime(quote1.datetime, "%Y-%m-%d %H:%M:%S.%f")  # 当前quote的时间
    now2 = datetime.datetime.strptime(quote1.datetime, "%Y-%m-%d %H:%M:%S.%f")  # 当前quote的时间

    if now1.day > now_day + 3:
        flag = cointegration_test(np.array(klines1['close']), np.array(klines2['close']))
        if not flag:
            target_pos1.set_target_volume(0)
            target_pos2.set_target_volume(0)
            print('协整检验不通过，无法交易')
            continue
        now_day = now1.day
        up, up_limit, up_limit2, down, down_limit, down_limit2 = get_param(klines1, klines2)

        print('更新参数')
    if not flag:
        continue

    time1 = time.ctime(klines2.iloc[-1].datetime / (10 ** 9))
    if api.is_changing(klines1):
        diff = klines1['close'].iloc[-1] - klines2['close'].iloc[-1]
        print(diff, klines1['close'].iloc[-1], klines2['close'].iloc[-1])
        # print(time1, ' diff:%.3f' % diff)
        if up_limit >= diff >= up:
            # print(klines1['close'].iloc[-1], klines2['close'].iloc[-1])
            while now1 == now2:
                api.wait_update()
                now2 = datetime.datetime.strptime(quote1.datetime, "%Y-%m-%d %H:%M:%S.%f")  # 当前quote的时间

            # print(now1, now2)
            # print(klines1['close'].iloc[-1], klines1['open'].iloc[-1])
            if api.get_position('SHFE.zn2005').pos == 0:
                target_pos1.set_target_volume(-1)
                target_pos2.set_target_volume(1)

        elif down_limit <= diff <= down:
            # print(klines1['close'].iloc[-1], klines2['close'].iloc[-1])
            while now1 == now2:
                api.wait_update()
                now2 = datetime.datetime.strptime(quote1.datetime, "%Y-%m-%d %H:%M:%S.%f")  # 当前quote的时间

            # print(now1, now2)
            # print(klines1['close'].iloc[-1], klines2['close'].iloc[-1])
            # print(klines1['open'].iloc[-1], klines2['open'].iloc[-1])
            if api.get_position('SHFE.zn2005').pos == 0:
                target_pos1.set_target_volume(1)
                target_pos2.set_target_volume(-1)

        elif diff < down_limit or diff > up_limit or (up_limit2 > diff > down_limit2):
            if api.get_position('SHFE.zn2005').pos != 0:
                if diff < down_limit or diff > up_limit:
                    print('止损卖出')
                else:
                    print('止盈卖出')

            while now1 == now2:
                api.wait_update()
                now2 = datetime.datetime.strptime(quote1.datetime, "%Y-%m-%d %H:%M:%S.%f")  # 当前quote的时间

            # print(now1, now2)

            target_pos1.set_target_volume(0)
            target_pos2.set_target_volume(0)
#
# print(time.ctime(klines1['datetime'].iloc[-1]/(10**9)))
