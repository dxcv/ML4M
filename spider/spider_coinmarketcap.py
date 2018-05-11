# -*- coding: utf-8 -*-
# @Author: Kai Zhang
# @Date:   2018-02-27 10:55:40

import os
import sys
import time
import requests
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), '..')))
from common.log import *
from common.config import Config

CONF = Config().data[0]
MONGODB = CONF['MONGODB']
NASDAQ = CONF['NASDAQ']
CRYPTOCURRENCY = CONF['CRYPTOCURRENCY']


class Spider_coinmarketcap(object):
    """Coinmarketcap数字加密货币的爬虫类"""

    def __init__(self):
        super(Spider_coinmarketcap, self).__init__()

    def get_coin_data(self, symbol, start_date, end_date):
        """
        Desc：
            通过coinmarketcap.com的historical-data页面，获取coin历史行情
        Parameter：
            coin：数字货币名称，如bitcoin、eos，以coinmarketcap的命名为准
            start_date：数据起始日期，字符串形式，如'20170101'
            end_date：数据结束日期，字符串形式，如time.strftime('%Y%m%d')、'20180101'
        Return：
            ret：True or False

        原理：
            因为coinmarketcap.com的historical-data页面的数据是HTML表格形式。
            所以直接存到本地database目录即可。
            提供给后续模块，用Pandas的read_html读取HTML文件提取数据。
        """
        info('Getting Coinmarketcap Market Index, symbol = %s, start_date = %s, end_date = %s' % (symbol, start_date, end_date))

        # 构造URL
        url = 'https://coinmarketcap.com/currencies/%s/historical-data/?start=%s&end=%s' % (symbol, start_date, end_date)
        debug(url)
        datafile = '../database/market/%s.html' % symbol
        info('Writting %s' % datafile)
        r = requests.get(url)
        with open(datafile, 'w+', encoding='utf-8') as f:
            f.write(r.text)
        return datafile


if __name__ == '__main__':
    set_log(INFO)
    spider = Spider_coinmarketcap()
    start_date = '20100101'
    end_date = time.strftime('%Y%m%d')
    for symbol in CRYPTOCURRENCY:
        datafile = spider.get_coin_data(symbol, start_date, end_date)
        info(datafile)
