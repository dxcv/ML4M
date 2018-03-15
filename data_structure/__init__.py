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
    def handle_coinmarketcap(coinmarketcap_html_file):
        """
        处理coinmarketcap的数字货币数据。
        输入：coinmarketcap的数据HTML文件，也就是Spider抓取回来的整个页面HTML
        输出：清洗后的DataFrame对象
        """

        # 读取coinmarketcap的html内容，转换成pandas.DataFrame
        debug(pd.Timestamp('now'))
        # coin_data = pd.read_html('../database/coinmarketcap_%s_%s_%s.html' % (coin, start_date, end_date))[0]
        coin_data = pd.read_html(coinmarketcap_html_file)[0]
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

        # 创建索引，正序排序
        coin_data.set_index(['Date'], inplace=True)
        coin_data = coin_data.sort_index(axis=0, ascending=True)
        debug(coin_data.head())

        return coin_data

    @staticmethod
    def handle_36kr(data_file):
        """
        处理36kr的新闻数据。
        输入：36kr的新闻txt文件，即Spider抓取回来的新闻，以Json字符串形式存储
        输出：清洗后的DataFrame对象
        """

        # 读取36kr新闻的Json字符串，转换成pandas.DataFrame
        with open(data_file, 'r', encoding='utf-8') as f:
            news_data = pd.read_json(f.read(), orient='records', typ='frame')

        # 输出新闻，有可能会出现gbk、unicode的编码问题
        # debug(news_data)
        debug(news_data.dtypes)

        # 把Date字段处理为日期
        news_data['publish_time'] = news_data['publish_time'].apply(lambda x: pd.Timestamp(x, freq='D'))
        # news_data['publish_time'] = news_data['publish_time'].apply(lambda x: pd.Period(x, freq='D'))

        # 创建索引
        news_data.set_index(['publish_time'], inplace=True)
        news_data = news_data.sort_index(axis=0, ascending=True)
        debug(news_data.head())
        debug(news_data.dtypes)

        return news_data

    def merge_data(self):
        return 'a pd.DataFrame'


if __name__ == '__main__':
    set_log(DEBUG)
    from data_structure import DataStructure as ds
    coinmarketcap_html_file = '../database/coinmarketcap_eos_20170101_20180314.html'
    coin_data = ds.handle_coinmarketcap(coinmarketcap_html_file)
    spider_36kr_data_file = '../database/36kr_EOS.txt'
    news_data = ds.handle_36kr(spider_36kr_data_file)
