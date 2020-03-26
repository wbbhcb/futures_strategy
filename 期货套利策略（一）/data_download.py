from datetime import datetime, date
from contextlib import closing
from tqsdk import TqApi, TqSim
from tqsdk.tools import DataDownloader

api = TqApi(TqSim())
download_tasks = {}

download_tasks["SHFE.cu1901"] = DataDownloader(api, symbol_list="SHFE.cu1901", dur_sec=5*60,
                    start_dt=date(2018, 1, 1), end_dt=date(2020, 1, 1), csv_file_name="cu1901.csv")

download_tasks["SHFE.cu1902"] = DataDownloader(api, symbol_list="SHFE.cu1902", dur_sec=5*60,
                    start_dt=date(2018, 1, 1), end_dt=date(2020, 1, 1), csv_file_name="cu1902.csv")

# 使用with closing机制确保下载完成后释放对应的资源
with closing(api):
    while not all([v.is_finished() for v in download_tasks.values()]):
        api.wait_update()
        print("progress: ", { k:("%.2f%%" % v.get_progress()) for k,v in download_tasks.items() })