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
from common.log import *


class DataStructure(object):
    """数据处理类，把爬虫数据变成结构化"""

    def __init__(self):
        super(DataStructure, self).__init__()

    @staticmethod
    def handle_coinmarketcap():
        """
        处理coinmarketcap的数字货币数据。
        输入：coinmarketcap的数据HTML文件
        输出：清洗后的DataFrame对象
        """
        coin = 'eos'
        start_date = '20170101'
        end_date = pd.Timestamp('today').strftime('%Y%m%d')

        # 读取coinmarketcap的html内容，转换成pandas.DataFrame
        debug(pd.Timestamp('now'))
        coin_data = pd.read_html('../database/coinmarketcap_%s_%s_%s.html' % (coin, start_date, end_date))[0]
        debug(coin_data.head())
        debug(coin_data.dtypes)
        debug(pd.Timestamp('now'))

        # 把Date字段处理为日期
        # coin_data = coin_data.assign(Date=pd.to_datetime(coin_data['Date']))  # cost 0.1s
        # coin_data['Date'] = coin_data['Date'].apply(lambda x: pd.Period(x, freq='D'))  # cost 0.2s
        coin_data['Date'] = coin_data['Date'].apply(lambda x: pd.Timestamp(x, freq='D'))  # cost 0.2s
        # coin_data['Date'] = coin_data['Date'].apply(lambda x: pd.to_datetime(coin_data['Date']))  # cost 12s
        debug(coin_data.head())
        debug(coin_data.dtypes)
        debug(pd.Timestamp('now'))

        # 把数据缺失的标记'-'转换为0
        debug(coin_data.tail())
        # coin_data.loc[coin_data['Volume'] == '-', 'Volume'] = 0
        # coin_data['Date'] = coin_data['Date'].apply(lambda x: pd.Timestamp(x, freq='D'))  # cost 0.2s
        coin_data = coin_data.replace(to_replace='-', value=0)  # cost 0.01s
        debug(pd.Timestamp('now'))
        debug(coin_data.tail())

        # 转换成正确的字段类型
        debug(coin_data.dtypes)
        # coin_data['Volume'] = coin_data['Volume'].astype('int64')  # cost 0.01s
        # coin_data['Market Cap'] = coin_data['Market Cap'].astype('int64')  # cost 0.01s
        coin_data = coin_data.astype(dtype={'Volume': 'int64', 'Market Cap': 'int64'})  # cost 0.01s
        debug(pd.Timestamp('now'))
        debug(coin_data.dtypes)

        # 创建索引
        coin_data.set_index(['Date'], inplace=True)
        coin_data = coin_data.sort_index(axis=0, ascending=True)
        debug(coin_data.head())

        return coin_data

    @staticmethod
    def handle_36kr():
        n = []
        keyword = 'EOS'
        with open('36kr_%s.txt' % keyword, 'r', encoding='utf-8') as f:
            for line in f:
                n.append(eval(line))
        return 'a pd.DataFrame'

    def merge_data(self):
        return 'a pd.DataFrame'


if __name__ == '__main__':
    set_log(DEBUG)
    from data_structure import DataStructure as ds
    coin_data = ds.handle_coinmarketcap()
