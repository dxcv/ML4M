# -*- coding: utf-8 -*-
# @Author: Kai Zhang
# @Date:   2018-04-10 14:17:40

import os
import sys
import datetime
import requests
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), '..')))
from common.log import *
from common.config import Config

CONF = Config().data[0]
MONGODB = CONF['MONGODB']
NASDAQ = CONF['NASDAQ']
CRYPTOCURRENCY = CONF['CRYPTOCURRENCY']


class Spider_nasdaq(object):
    """Nasdaq股数据的爬虫类"""

    def __init__(self):
        pass

    def get_stock_data(self, symbol, timeframe, save_csv=False):
        """
        Desc:
            通过Nasdaq官网获取Nasdaq股票数据，保存为HTML文件。
            最好一次性获取10年（10y）数据，因为数据并不大。
        Parameter:
            stock: 字符串，股票代码
            timeframe: 时间区间，只能按照Nasdaq官网的规定，如5d、1m、3m、6m、18m、1y、2y...10y。
            save_csv: 无用参数，脚本无法下载csv类型数据，默认为False。
        Return:
            无。
        原理:
            通过Nasdaq官网获取Nasdaq股票数据。
            由于安全限制，脚本只能获取到HTML，不能获取的csv类型数据。
        """
        info('Getting Nasdaq Stock Index, symbol = %s, timeframe = %s' % (symbol, timeframe))

        # 构造参数
        url = 'https://www.nasdaq.com/symbol/%s/historical' % symbol
        data = '%s|%s|%s' % (timeframe, symbol.lower(), save_csv)
        headers = {'Content-Type': 'application/json'}

        # 将数据时长转换成时间区间
        # 因为有时差，所以获取到最新的是昨天的数据
        # 1个月暂定30天，只影响文件名，具体数据日期是看文件里的真实数据
        day_per_unit = {'d': 1, 'm': 30, 'y': 365}
        days = int(timeframe[:-1]) * day_per_unit[timeframe[-1]]
        yesterday = datetime.datetime.today() - datetime.timedelta(days=1)
        start_day = yesterday - datetime.timedelta(days=days)
        yesterday = yesterday.strftime('%Y%m%d')
        start_day = start_day.strftime('%Y%m%d')

        # 下载保存数据
        r = requests.post(url, data=data, headers=headers)
        datafile = '../database/market/%s.html' % symbol
        info('Writting %s' % datafile)
        with open(datafile, 'w+', encoding='utf-8') as f:
            f.write(r.text)
        return datafile


if __name__ == '__main__':
    set_log(INFO)
    spider = Spider_nasdaq()
    timeframe = '10y'
    for symbol in NASDAQ:
        datafile = spider.get_stock_data(symbol, timeframe)
        info(datafile)
