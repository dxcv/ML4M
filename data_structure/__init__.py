# -*- coding: utf-8 -*-
# @Author: Kai Zhang
# @Date:   2018-03-13 16:28:18

import os
import requests
import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sn
import sys
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), '..')))


class DataStructure(object):
    """数据处理类，把爬虫数据变成结构化"""

    def __init__(self):
        super(DataStructure, self).__init__()

    @staticmethod
    def handle_coinmarketcap():
        coin = 'eos'
        start_date = '20170101'
        end_date = pd.Timestamp('today').strftime('%Y%m%d')

        # 读取coinmarketcap的html内容，转换成pandas.DataFrame
        print(pd.Timestamp('now'))
        coin_data = pd.read_html('../database/coinmarketcap_%s_%s_%s.html' % (coin, start_date, end_date))[0]
        print(coin_data.head())
        print(coin_data.dtypes)
        print(pd.Timestamp('now'))

        # 把Date字段处理为日期
        # coin_data = coin_data.assign(Date=pd.to_datetime(coin_data['Date']))  # cost 0.1s
        # coin_data['Date'] = coin_data['Date'].apply(lambda x: pd.Period(x, freq='D'))  # cost 0.2s
        coin_data['Date'] = coin_data['Date'].apply(lambda x: pd.Timestamp(x, freq='D'))  # cost 0.2s
        # coin_data['Date'] = coin_data['Date'].apply(lambda x: pd.to_datetime(coin_data['Date']))  # cost 12s
        print(coin_data.head())
        print(coin_data.dtypes)
        print(pd.Timestamp('now'))

        # 把数据缺失的标记'-'转换为0
        print(coin_data.tail())
        # coin_data.loc[coin_data['Volume'] == '-', 'Volume'] = 0
        return coin_data

    def handle_36kr(self):
        return 'a pd.DataFrame'

    def merge_data(self):
        return 'a pd.DataFrame'


if __name__ == '__main__':
    from data_structure import DataStructure as ds
    coin_data = ds.handle_coinmarketcap()
