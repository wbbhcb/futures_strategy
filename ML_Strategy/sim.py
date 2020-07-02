import tushare as ts
import numpy as np
import datetime
from tqsdk import TqApi, TqBacktest, TargetPosTask, TqSim, TqAccount
import time
import warnings
import pickle
from config import config
import utils
from feature_compute import extract_feature
from ZhuLi_HeYue import ZhuLi_HeYue
warnings.filterwarnings('ignore')

token = config.ts_token
pro = ts.pro_api(token)
backtest = config.back_test

#############################
# 提取有哪些交易日###########
#############################
if backtest:
    start_date = config.start_date
    end_date = config.end_date
    t1 = start_date - datetime.timedelta(days=30)
    t2 = end_date + datetime.timedelta(days=30)
else:
    t2 = datetime.datetime.now()
    if t2.hour > 15:
        t2 = t2 + datetime.timedelta(days=1)
    t1 = t2 - datetime.timedelta(days=30)

t1 = str(t1.year)+str(t1.month).zfill(2)+str(t1.day).zfill(2)
t2 = str(t2.year)+str(t2.month).zfill(2)+str(t2.day).zfill(2)
df = pro.query('trade_cal', exchange='DCE', start_date=t1, end_date=t2)
trade_dates = df[df['is_open'] == 1]['cal_date'].unique()
trade_dates = list(trade_dates)
last_time_2 = None


if __name__ == '__main__':
    if backtest:
        acc = TqSim(init_balance=1000000)
        print(start_date, end_date)
        api = TqApi(acc, backtest=TqBacktest(start_dt=start_date, end_dt=end_date), web_gui='127.0.0.1:9999')
        quote1 = api.get_quote('SHFE.ag2005')
    else:
        api = TqApi(TqAccount("快期模拟", "******", "*******"))

    # load model
    model_path = 'model/model.pickle'
    with open(model_path, 'rb+') as f:
        clf = pickle.load(f)

    while True:
        if backtest:
            now_time_ = datetime.datetime.strptime(quote1.datetime, "%Y-%m-%d %H:%M:%S.%f")
            trade_date = str(now_time_.year) + str(now_time_.month).zfill(2) + str(now_time_.day).zfill(2)
            zhuli = ZhuLi_HeYue(trade_date=trade_date, now_time=now_time_)
        else:
            now_time_ = datetime.datetime.now()
            zhuli = ZhuLi_HeYue()

        if last_time_2 != now_time_:
            last_time_2 = now_time_
            df_list = zhuli.get_zhuli()
            pre_close_dict = zhuli.get_info(trade_dates)
            if backtest:
                print('a new day: %s' % trade_date)

            klines = []  # 所有K线保存在这
            target_pos = []  # 所有任务TargetPossTask放这
            hold_pos = []  # 持仓变换情况
            names = []  # 名字
            last_times = []  # 上一交易日时刻， 不同合约交易时段不同

            for kind in df_list['code'].unique():
                tmp_k = api.get_kline_serial([kind], 60, data_length=1200)
                if 'ag' in kind:
                    # 应该每天都会有ag合约，否则程序会出错
                    sp_kline = tmp_k
                    last_time_ = utils.transform_datetime(sp_kline['datetime'].values[-1])

                target_pos.append(TargetPosTask(api, kind))
                names.append(kind)
                klines.append(tmp_k)
                last_times.append(utils.transform_datetime(tmp_k['datetime'].values[-1]))

            hold_kind = []
            hold_time = []
            direction = []

        t1 = time.time()
        while True:
            api.wait_update()
            t2 = time.time()
            print(last_time_, t2-t1)
            t1 = t2
            if last_time_ != utils.transform_datetime(sp_kline['datetime'].values[-1]):
                # 表示新的一分钟

                # 判断是否每个合约都更新了，不同合约交易时间不一样
                update_kind = set()
                for i in range(len(last_times)):
                    if last_times[i] != utils.transform_datetime(klines[i]['datetime'].values[-1]):
                        last_times[i] = utils.transform_datetime(klines[i]['datetime'].values[-1])
                        update_kind.add(names[i])

                # 更新持有时间
                for i in range(len(hold_kind)):
                    if hold_kind[i] in update_kind:
                        hold_time[i] = hold_time[i] + 1

                if last_time_.hour == 21 and last_time_.minute == 19:
                    break

                last_time_ = utils.transform_datetime(sp_kline['datetime'].values[-1])

                if not (last_time_.hour == 14 and last_time_.minute >= 40):
                    # 14点40以后不交易
                    klines2 = utils.process(klines, pre_close_dict, trade_dates)  # 对原来的klines匹配pre_close等数值
                    name_list, features = extract_feature(klines2)  # 特征计算
                    prob = clf.predict(features)
                    long = np.where(prob > config.buying_prob)[0]
                    short = np.where(prob < config.selling_prob)[0]
                    hold_kind, hold_time, direction = utils.trade(api, name_list, short, long, hold_kind, hold_time,
                                                                  direction, update_kind, names, target_pos)
                # break

            if last_time_.hour == 14 and last_time_.minute >= 57:
                utils.Clearance(api, target_pos, names)
                hold_kind = []
                hold_time = []
                direction = []

            if last_time_.hour == 14 and last_time_.minute == 59:
                # 新的一天，重新获取主力合约
                break
        # if last_time_.hour == 21 and last_time_.minute == 19 and last_time_.day == 2:
        #     break
        if not backtest:
            # 如果不在回测表明当天已经结束
            break
#
#     # t1=time.time()
#     # t2=time.time()
#     # print(t2-t1)
#     # for i in range(len(klines2)):


