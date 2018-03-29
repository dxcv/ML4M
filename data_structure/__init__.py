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
        coin_data['Date'] = coin_data['Date'].apply(lambda x: pd.Period(x, freq='D'))  # cost 0.2s
        # coin_data['Date'] = coin_data['Date'].apply(lambda x: pd.Timestamp(x, freq='D'))  # cost 0.2s
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
        # news_data['publish_time'] = news_data['publish_time'].apply(lambda x: pd.Timestamp(x, freq='D'))
        news_data['publish_time'] = news_data['publish_time'].apply(lambda x: pd.Period(x, freq='D'))

        # 创建索引
        # news_data.set_index(['publish_time'], inplace=True)
        # news_data = news_data.sort_index(axis=0, ascending=True)
        debug(news_data.head())
        debug(news_data.dtypes)

        return news_data

    @staticmethod
    def merge_data(coin_data, news_data):
        """
        合并数据货币和新闻数据。
        输入：数据货币DataFrame、新闻数据DataFrame
        输出：合并、处理后的DataFrame
        """
        news_data = news_data.groupby('publish_time').size().to_frame(name='counts')
        debug(news_data.tail(10))
        debug(news_data.dtypes)

        # 用Merge等同于Join
        result_df = pd.merge(coin_data, news_data, how='left', left_index=True, right_index=True)
        result_df = result_df.loc[:, ['Close', 'Volume', 'counts']]
        # result_df = result_df.loc[:, ['Close', 'Volume', 'counts']]
        result_df = result_df.fillna(value={'counts': 0})
        debug(result_df.tail(10))
        return result_df

    @staticmethod
    def draw_line(result_df):
        # result_df['counts'] = result_df['counts'].apply(lambda x: x * 10)
        ax = result_df.plot(kind='line', y='Close', label='Close', color='DarkBlue')
        ax = result_df.plot(kind='line', y='counts', label='counts', secondary_y=True, color='LightGreen', ax=ax)
        # result_df = result_df.cumsum()
        # print(result_df)
        # plt.figure()
        # result_df.plot()
        plt.show()


if __name__ == '__main__':
    set_log(INFO)
    from data_structure import DataStructure as ds
    coinmarketcap_html_file = '../database/coinmarketcap_eos_20170101_20180327.html'
    coin_data = ds.handle_coinmarketcap(coinmarketcap_html_file)
    # spider_36kr_data_file = '../database/36kr_EOS.txt'
    spider_36kr_data_file = '../database/36kr_区块链.txt'
    news_data = ds.handle_36kr(spider_36kr_data_file)
    result_df = ds.merge_data(coin_data, news_data)
    ds.draw_line(result_df)

    from machine_learning.kNN import kNN

    result_df = kNN.autoNorm(result_df)
    print(result_df)

    result_df = result_df.reset_index(drop=True)
    result_df['Close_change'] = 'UP'
    for i in range(1, len(result_df), 1):
        result_df.loc[i, 'Close_change'] = 'UP' if result_df.loc[i, 'Close'] > result_df.loc[i - 1, 'Close'] else 'DOWN'
    result_df.set_index(['Close_change'], inplace=True)
    print(result_df)

    hoRatio = 0.5
    numTestVecs = int(hoRatio * len(result_df))
    errorCount = 0
    for i in range(numTestVecs):
        inX_lable = kNN.classify0(result_df[i:i + 1], result_df[0:], 10)
        # info('Point %d, correct_label=%s, kNN_label=%s' % (i, inX_lable, result_df.iloc[i, :].name))
        # debug(pd.Timestamp('now'))
        if inX_lable != result_df.iloc[i, :].name:
            errorCount += 1
    info('numTestVecs: %d' % numTestVecs)
    info('errorCount: %d' % errorCount)
    info('errorRate: %f' % float(errorCount / numTestVecs))

    price_up = result_df.loc[result_df.index == 'UP']
    price_down = result_df.loc[result_df.index == 'DOWN']

    ax = price_up.plot.scatter(
        x='Volume', y='counts', label='price_up', color='DarkBlue')
    ax = price_down.plot.scatter(
        x='Volume', y='counts', label='price_down', color='LightGreen', ax=ax)

    plt.show()
