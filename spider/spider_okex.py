# -*- coding: utf-8 -*-
# @Author: Kai Zhang
# @Date:   2019-06-03 11:18:22

import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), '..')))

import time
import requests
import pandas as pd
from common.log import *
from common.config import Config

CONF = Config().data[0]
MONGODB = CONF['MONGODB']
NASDAQ = CONF['NASDAQ']
CRYPTOCURRENCY = CONF['CRYPTOCURRENCY']


class Spider_okex(object):
    """OKEX交易所数字加密货币的爬虫类"""

    def __init__(self):
        super(Spider_okex, self).__init__()

    def get_coin_data(self, symbol, lite=False):
        """
        Desc：
            通过OKEX APIv3的/instruments/XXX-XXX/candles接口获取OKEX历史行情。
            此处行情并非美元行情，而是COIN-USDT币币交易对行情。
            法币行情意义不大，没有操作空间，用USDT稳定币才有意义。
        Parameter：
            symbol：数字货币代码，如BTC、EOS。
        Return：
            datafile：行情保存在本地化的文件路径。

        原理：
            OKEX APIv3的/instruments/XXX-XXX/candles的数据是JSON格式。
            但每次都最多返回200条数据。
            因此需要轮询分日期段请求。
        """
        info('Getting OKEX Market Index, symbol = %s' % symbol)

        year_now = pd.Timestamp.today().year
        date_list = []

        # 生成时间区间节点
        if lite:
            start_year = year_now
        else:
            start_year = 2017
        for y in range(start_year, year_now + 1):
            date_list.append('%d-01-01T00%%3A00%%3A00.000Z' % y)
            date_list.append('%d-07-01T00%%3A00%%3A00.000Z' % y)
        date_list.append('%d-01-01T00%%3A00%%3A00.000Z' % (y + 1))

        # 构造URL
        url_list = []
        for i in range(len(date_list) - 1):
            start_date = date_list[i]
            end_date = date_list[i + 1]
            url = 'https://www.okex.me/api/spot/v3/instruments/%s-USDT/candles?granularity=86400&start=%s&end=%s' % (symbol, start_date, end_date)
            debug(url)
            url_list.append(url)
        url_list.reverse()
        if lite:
            url_list = ['https://www.okex.me/api/spot/v3/instruments/%s-USDT/candles?granularity=86400' % symbol]

        # 构造headers
        # headers = {
        #     'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36',
        # }

        # 请求数据
        df = pd.DataFrame()
        for url in url_list:
            time.sleep(1)
            debug(url)
            _df = pd.read_json(url)
            debug(len(_df))
            if len(df) == 0:
                df = _df
            else:
                df = df.append(_df, ignore_index=True)

        # 处理数据
        df.columns = ['date', 'open', 'high', 'low', 'close', 'volumn']
        df['date'] = df['date'].apply(pd.to_datetime)
        df['date'] = df['date'] + pd.Timedelta(hours=8)
        debug(df.head(5))
        debug(df.tail(5))

        # 保存数据
        if lite:
            datafile = '../database/market/%s_OKEX_lite.csv' % symbol
        else:
            datafile = '../database/market/%s_OKEX.csv' % symbol
        info('Writting %s' % datafile)
        df.to_csv(datafile, header=['date', 'open', 'high', 'low', 'close', 'volume'], index=False, encoding='utf-8')
        return datafile

    def update_coin_data(self, symbol):
        pass


if __name__ == '__main__':
    set_log(DEBUG)
    spider = Spider_okex()
    spider.get_coin_data('EOS', lite=True)
    # spider.update_coin_data('EOS')
    # for symbol in CRYPTOCURRENCY:
    #     datafile = spider.get_coin_data(symbol, start_date, end_date)
    #     info(datafile)
