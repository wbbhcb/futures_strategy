import tushare as ts
import pandas as pd
import numpy as np
import datetime
from tqdm import tqdm
import os
import time
from config import config
import warnings
import copy
warnings.filterwarnings('ignore')


def transform_datetime(x):
    month = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    s = time.ctime(x / 10 ** 9)
    tmp_list = s.split(' ')
    for tmp in tmp_list:
        if tmp == '':
            tmp_list.remove(tmp)
    m = month.index(tmp_list[1])
    h = int(tmp_list[3][:2])
    minute = int(tmp_list[3][3:5])
    seconds = int(tmp_list[3][6:8])
    day = datetime.datetime(int(tmp_list[-1]), m+1, int(tmp_list[2]), h, minute, 0)
    if seconds > 45:
        day = day + datetime.timedelta(minutes=1)
    # day = str(tmp_list[-1]) + str(m + 1).zfill(2) + str(tmp_list[2]).zfill(2)
    return day


def get_tradedate(t, trade_dates):
    # if t.hour > 20 or t.hour < 8:
    #     if not config.back_test:
    #         t = datetime.datetime.now()
    #     if t.hour > 20:
    #         t = t + datetime.timedelta(days=1)

    day = str(t.year) + str(t.month).zfill(2) + str(t.day).zfill(2)
    if t.hour > 15:
        try:
            idx = trade_dates.index(day)
            day = trade_dates[idx + 1]
        except:
            # 说明在非交易日调试
            day = trade_dates[-1]

    # 在凌晨的时候，周六凌晨也会交易
    if t.hour < 8:
        if day not in trade_dates:
            # 说明是在非工作日凌晨交易
            t = t - datetime.timedelta(days=1)
            day = str(t.year) + str(t.month).zfill(2) + str(t.day).zfill(2)
            idx = trade_dates.index(day)
            day = trade_dates[idx + 1]
    return day


def process(klines, pre_close_dict, trade_dates):
    # 计算一些参数
    # t1 = time.time()
    klines2 = copy.deepcopy(klines)
    for i in range(len(klines2)):
        kind = klines2[i]['symbol'][0]
        tmp_k = klines2[i]
        tmp_k['mean'] = (tmp_k['open'] + tmp_k['close'] + tmp_k['high'] + tmp_k['low']) / 4
        tmp_k['day'] = tmp_k['datetime'].apply(transform_datetime)
        # transform_datetime(tmp_k['datetime'].values[-1])
        tmp_k['trade_date'] = tmp_k['day'].apply(lambda x: get_tradedate(x, trade_dates))
        tmp_k['pre_close_day'] = tmp_k['trade_date'].map(pre_close_dict[kind])
        tmp_k['cumvol'] = tmp_k.groupby('trade_date')['volume'].cumsum()
        tmp_k['rate'] = (tmp_k['mean'] - tmp_k['pre_close_day']) / tmp_k['pre_close_day']
        klines2[i] = tmp_k
    # t2 = time.time()
    # print(t2-t1)
    return klines2


def trade(api, name_list, short, long, hold_kind, hold_time, direction, update_kind, names, target_pos):
    # 目前可能会出现挂单后交易不成功的情况，暂不考虑

    hold_pos = []
    for i in range(len(names)):
        kind = names[i]
        hold_pos.append(api.get_position(kind).pos)
    hold_pos2 = hold_pos.copy()

    # 做空
    for i in range(len(short)):
        if name_list[short[i]] in update_kind:
            hold_kind.append(name_list[short[i]])
            hold_time.append(0)
            direction.append(-1)
            idx = names.index(name_list[short[i]])
            hold_pos2[idx] = hold_pos2[idx] - 1

    # 做多
    for i in range(len(long)):
        if name_list[long[i]] in update_kind:
            hold_kind.append(name_list[long[i]])
            hold_time.append(0)
            direction.append(1)
            idx = names.index(name_list[long[i]])
            hold_pos2[idx] = hold_pos2[idx] + 1

    # 到时间卖出

    for i in range(len(hold_kind)-1, -1, -1):

        if hold_time[i] >= 19:
            idx = names.index(hold_kind[i])
            if direction[i] == 1:
                hold_pos2[idx] = hold_pos2[idx] - 1
            else:
                hold_pos2[idx] = hold_pos2[idx] + 1
            print('time end, 平1手  ' + hold_kind[i])
            hold_kind.pop(i)
            hold_time.pop(i)
            direction.pop(i)
            api.wait_update()


    # 挂单
    for i in range(len(hold_pos)):
        if hold_pos[i] != hold_pos2[i]:
            target_pos[i].set_target_volume(hold_pos2[i])
    api.wait_update()

    return hold_kind, hold_time, direction


def Clearance(api, target_pos, names):
    for i in range(len(names)):
        kind = names[i]
        if api.get_position(kind).pos != 0:
            target_pos[i].set_target_volume(0)
    api.wait_update()
