import pandas as pd
import numpy as np
import datetime


def index_feature(klines, get_history=False):
    # get_history: 表示是否获得历史的指数信息
    df_list = []
    cols = ['day', 'rate', 'cumvol', 'pre_close_day', 'mean']
    for i in range(len(klines)):
        if get_history:
            df_list.append(klines[i][cols])
        else:
            df_list.append(klines[i][cols].iloc[-10:])
    df_list = pd.concat(df_list)

    def get_index(x):
        index_rate = np.sum(x['rate'] * x['cumvol']) / np.sum(x['cumvol'])
        return index_rate

    df_list = df_list.groupby('day').apply(get_index).reset_index()
    df_list.columns = ['day', 'index_rate']
    return df_list


def normal_feature(klines):
    name_list = []
    features = []

    for tmp_k in klines:
        tmp_feature = []
        name_list.append(tmp_k['symbol'][0])

        # 剔除最后一分钟的数据，此时最后一分钟还没跑完
        mean_price = tmp_k['mean'].values[:-1]
        high_price = tmp_k['high'].values[:-1]
        open_price = tmp_k['open'].values[:-1]
        low_price = tmp_k['low'].values[:-1]
        volume = tmp_k['volume'].values[:-1]
        pre_close = tmp_k['pre_close_day'].values[:-1]

        for day in [5, 10, 15, 30, 60]:
            s = np.max(high_price[-day:])
            s = (mean_price[-1] - s) / s
            tmp_feature.append(s)

            s = np.min(low_price[-day:])
            s = (mean_price[-1] - s) / s
            tmp_feature.append(s)

            s = np.mean(mean_price[-day:])
            s = (mean_price[-1] - s) / s
            tmp_feature.append(s)

            s = np.sum(volume[-day:])
            s = (volume[-1] - s) / s
            tmp_feature.append(s)

        for day in [1, 2, 3]:
            s = (high_price[-(day+1)] - mean_price[-1]) / mean_price[-1]
            tmp_feature.append(s)

            s = (low_price[-(day+1)] - mean_price[-1]) / mean_price[-1]
            tmp_feature.append(s)

            s = (mean_price[-(day+1)] - mean_price[-1]) / mean_price[-1]
            tmp_feature.append(s)

            s = (high_price[-(day+1)] - pre_close[-1]) / pre_close[-1]
            tmp_feature.append(s)

            s = (low_price[-(day+1)] - pre_close[-1]) / pre_close[-1]
            tmp_feature.append(s)

            s = (mean_price[-(day+1)] - pre_close[-1]) / pre_close[-1]
            tmp_feature.append(s)

        s = (high_price[-1] - pre_close[-1]) / pre_close[-1]
        tmp_feature.append(s)
        s = (low_price[-1] - pre_close[-1]) / pre_close[-1]
        tmp_feature.append(s)
        s = (open_price[-1] - pre_close[-1]) / pre_close[-1]
        tmp_feature.append(s)

        features.append(tmp_feature)

    return name_list, np.array(features)


def extract_feature(klines):
    index_df = index_feature(klines, get_history=False)
    name_list, features = normal_feature(klines)
    df = pd.DataFrame(features)

    index_rate = index_df['index_rate'].values[:-1]  # 剔除最后一分钟数据

    for i in range(5):
        df['index_rate_'+str(i)] = index_rate[-(i+1)]
    features = df.values

    if np.isnan(features).any():
        with open('log.txt', 'a+') as f:
            f.write(str(index_df['day'].values[-1]) + '\n')

    return name_list, features