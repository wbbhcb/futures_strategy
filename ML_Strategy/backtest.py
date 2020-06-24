from datetime import date
import datetime
from tqsdk import TqApi, TqBacktest, TargetPosTask, TqSim
import pandas as pd
import os
import time
import gc

buying_prob = 0.8
selling_prob = 0.2
acc = TqSim(init_balance=1000000)
start_date = date(2019, 9, 1)
end_date = date(2020, 4, 20)
# 在创建 api 实例时传入 TqBacktest 就会进入回测模式
api = TqApi(acc, backtest=TqBacktest(start_dt=start_date, end_dt=end_date), web_gui='127.0.0.1:9999')

base_path = 'data'
pred = pd.read_csv(os.path.join(base_path, 'result.csv'))

pred = pred.sort_values('datetime').reset_index(drop=True)
pred['datetime'] = pd.to_datetime(pred['datetime'], infer_datetime_format=True)
pred['datetime_open'] = pd.to_datetime(pred['datetime_open'], infer_datetime_format=True)
pred['datetime_close'] = pd.to_datetime(pred['datetime_close'], infer_datetime_format=True)
pred = pred[(pred['pred']>buying_prob) | (pred['pred']<selling_prob)].reset_index(drop=True)
# pred['is_sc'] = pred['ts_code'].apply(lambda x:True if 'sc' in x else False)
# pred = pred[pred['is_sc'] == False].reset_index()
# pred = pred[(pred['datetime_open'] >= datetime.datetime(2020, 2, 3))&(pred['datetime_open']<=datetime.datetime(2020, 2, 5))]
pred['datetime'] = pred['datetime'] + pd.Timedelta(seconds=60)


# klines1 = api.get_kline_serial(['SHFE.ag1912'], 60, data_length=20)
num = 0

hold_kind = []  # 持有的合约
direction = []  # 方向，做空还是做多
trade_kind = []  #
close_datetime = []  # 平仓时间
# set_kind = set()
t1 = time.time()
last_times = dict()
now_times = dict()
quote = dict()

quote1 = api.get_quote('SHFE.ag2005')
last_time_ = datetime.datetime.strptime(quote1.datetime, "%Y-%m-%d %H:%M:%S.%f")
now_time_ = datetime.datetime.strptime(quote1.datetime, "%Y-%m-%d %H:%M:%S.%f")
api.wait_update()

for trade_date, tmp_df in pred.groupby('datetime_open'):
    # print(now_time_, now_time_ - trade_date)
    # print(trade_date)
    tmp_df = tmp_df[(tmp_df['pred'] > buying_prob)|(tmp_df['pred'] < selling_prob)]
    flag = False  # 判断是否有进行加减仓等一系列操作

    if len(tmp_df) != 0:
        trade_kind = list(tmp_df['ts_code'].values)
        trade_prob = tmp_df['pred'].values
        close_time = list(tmp_df['datetime_close'])

        # for code in tmp_df['ts_code'].unique():
        #     if code not in quote:
        #         quote_ = api.get_quote(code)
        #         quote[code] = quote_
        #         now_times[code] = datetime.datetime.strptime(quote_.datetime, "%Y-%m-%d %H:%M:%S.%f")
    else:
        continue

    while True:
        if flag:
            api.wait_update()
        now_time_ = datetime.datetime.strptime(quote1.datetime, "%Y-%m-%d %H:%M:%S.%f")

        # print(datetime.datetime.strptime(quote1.datetime, "%Y-%m-%d %H:%M:%S.%f"))
        # 进行做多或者做空操作
        if pd.Timedelta(seconds=-15) < trade_date - now_time_ < pd.Timedelta(seconds=15):
            for kind, prob, tmp_close_time in zip(trade_kind, trade_prob, close_time):
                target_pos = TargetPosTask(api, kind)
                hold_pos = api.get_position(kind).pos
                if prob > buying_prob:
                    target_pos.set_target_volume(hold_pos + 1)
                    direction.append(1)
                else:
                    target_pos.set_target_volume(hold_pos - 1)
                    direction.append(-1)
                hold_kind.append(kind)
                close_datetime.append(tmp_close_time)
                # set_kind.add(kind)
                print(kind, direction[-1], now_time_)
                api.wait_update()
            flag = True
            trade_kind = []
            trade_prob = []
            # break
        elif now_time_ - trade_date > pd.Timedelta(seconds=15):
            break
        # elif

        # 查看是否已满20分钟
        while True:
            if len(hold_kind) == 0:
                break
            # print(now_time-hold_time[0])
            for i in range(len(hold_kind)-1, -1, -1):
                if now_time_ - close_datetime[i] >= pd.Timedelta(seconds=-15):
                    target_pos = TargetPosTask(api, hold_kind[i])
                    hold_pos = api.get_position(hold_kind[i]).pos
                    if direction[i] == 1 and hold_pos != 0:
                        target_pos.set_target_volume(hold_pos - 1)
                    elif hold_pos != 0:
                        target_pos.set_target_volume(hold_pos + 1)
                    print('time end, 平1手  ' + hold_kind[i])
                    hold_kind.pop(i)
                    direction.pop(i)
                    close_datetime.pop(i)
                    api.wait_update()
            flag = True
            break

        # 当日清仓
        if now_time_.hour == 14 and now_time_.minute >= 58:
            # tmp_kind = []
            # print(api._serials.items())
            print(now_time_)
            for tmp in set(hold_kind):
                target_pos = TargetPosTask(api, tmp)
                if target_pos != 0:
                    target_pos.set_target_volume(0)
                    print('清空 '+str(tmp))
                    api.wait_update()
            flag = True
            hold_kind = []
            close_datetime = []
            direction = []
            # set_kind = set()


t2 = time.time()
print(t2 - t1)
filename = 'result.txt'
with open(filename, 'w') as f:  # 如果filename不存在会自动创建， 'w'表示写数据，写之前会清空文件中的原有数据！
    f.write(str(t2 - t1))
    
########
# Step 5
########
while True:
    api.wait_update()
    now_time_ = datetime.datetime.strptime(quote1.datetime, "%Y-%m-%d %H:%M:%S.%f")

    # 查看是否已满20分钟
    while True:
        if len(hold_kind) == 0:
            break
        # print(now_time-hold_time[0])
        for i in range(len(hold_kind)-1, -1, -1):
            if now_time_ - close_datetime[i] >= pd.Timedelta(seconds=-15):
                target_pos = TargetPosTask(api, hold_kind[i])
                hold_pos = api.get_position(hold_kind[i]).pos
                if direction[i] == 1 and hold_pos != 0:
                    target_pos.set_target_volume(hold_pos - 1)
                elif hold_pos != 0:
                    target_pos.set_target_volume(hold_pos + 1)
                print('time end, 平1手  ' + hold_kind[i])
                hold_kind.pop(i)
                direction.pop(i)
                close_datetime.pop(i)
                api.wait_update()
        break

    # 当日清仓
    if now_time_.hour == 14 and now_time_.minute >= 58:
        # tmp_kind = []
        # print(api._serials.items())
        print(now_time_)
        for tmp in set(hold_kind):
            target_pos = TargetPosTask(api, tmp)
            if target_pos != 0:
                target_pos.set_target_volume(0)
                print('清空 ' + str(tmp))
                api.wait_update()
        hold_kind = []
        close_datetime = []
        direction = []
        # set_kind = set()


