# 注意
如果没有tushare积分无法下载数据的话，可以关注公众号，后台回复“期货数据”，即可获取期货数据

# 教程链接
期货ML策略(一)数据获取：
https://mp.weixin.qq.com/s/2h0sEMeT_Al2w_mGoT9mKQ

期货ML策略（二）构建机器学习模型：
https://mp.weixin.qq.com/s/aOaptZZRKHGFETKjco05SA

期货ML策略（三）基于交易信号的回测：
https://mp.weixin.qq.com/s/xpa1XQ9JqMkVdVPhjLdvJw

期货ML策略（四）模拟盘/实盘上线
https://mp.weixin.qq.com/s/BOGEzjoolx8OrRNBnFzTyw

# 代码说明
data_download.py：数据下载，需要替换自己的tushare token

data_index.py：指数信息合成

Train.ipynb：训练ML模型，同时输出交易信号

backtest.py：基于交易信号的回测

**以下是模拟盘/实盘代码**

config.py：参数设置 如果是实盘或模拟盘, basktest需要设置false, startdate和enddate是回测的时间范围

ZhuLi_HeYue.py：主力合约类  获取主力合约列表和主力合约历史日K信息，需要tushare token，大家也可通过 天勤平台获取，根据收盘持仓量大小判断

utils.py：一些工具函数，比如trade等

feature_compute.py：特征计算

sim.py：模拟盘/实盘/回测代码，如果模拟时可以自己注册模拟盘账户，实盘则需要开户并修改成实盘账户即可，很方便

**以上代码可能会有部分问题，还没经过模拟盘的检验，如有问题大家可以自行修改**

# 结语
如果觉得代码帮助很大，希望给个星，谢谢支持！！！

如果对个人在量化上的研究感兴趣可以关注个人公众号（公众号上有个人对代码的讲解）,不定期分享一些研究情况.后期策略成熟会分享一些股票.

公众号:**Gambler_Evolution**

 ![image](https://github.com/wbbhcb/futures_strategy/blob/master/qrcode.jpg)

个人知乎:https://www.zhihu.com/people/e-zhe-shi-wo/activities
