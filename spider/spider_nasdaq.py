# -*- coding: utf-8 -*-
# @Author: Kai Zhang
# @Date:   2018-04-10 14:17:40

import os
import sys
import datetime
import requests
import pandas as pd
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

    def get_stock_data(self, symbol, timeframe, save_csv='false'):
        """
        Desc:
            通过Nasdaq官网获取Nasdaq股票数据，保存为HTML文件。
            最好一次性获取10年（10y）数据，因为数据并不大。
        Parameter:
            stock: 字符串，股票代码
            timeframe: 时间区间，只能按照Nasdaq官网的规定，如5d、1m、3m、6m、18m、1y、2y...10y。
            save_csv: 字符串，默认为false，用于获取Nasdaq的csv格式数据。
        Return:
            无。
        原理:
            通过Nasdaq官网获取Nasdaq股票数据。
            可通过字段控制获取csv或html类型数据。
            目前不支持获取csv类型数据，而是获取html类型数据，然后用pandas保存为csv数据文件。
        """
        info('Getting Nasdaq Stock Index, symbol = %s, timeframe = %s' % (symbol, timeframe))

        # 构造参数
        url = 'https://www.nasdaq.com/symbol/%s/historical' % symbol.lower()
        save_csv = 'false'
        # submitString = '%s|%s|%s' % (timeframe, save_csv, symbol.lower())
        submitString = '%s|%s|%s' % (timeframe, symbol.lower(), save_csv)
        headers = {
            'Content-Type': 'application/json',
            # 'Content-Type': 'application/x-www-form-urlencoded',
            # 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36',
        }

        # data = {
        #     '__VIEWSTATE': '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',
        #     '__VIEWSTATEGENERATOR': '87BE3BDB',
        #     '__VIEWSTATEENCRYPTED': '',
        #     '__EVENTVALIDATION': 'zCDdyPdLE7K6hvBcQkx8IqmdYuohOiOwoDMUW8EYwP2Sbo3yoOJRGbOYnqy47J9pOfDnkqel5MJReTUcOHR8hQsybBYFTPKSUxmW+qIags460zEFH82lLny24P9nJQPf',
        #     'ctl00$quotes_content_left$submitString': submitString,
        # }

        # 将数据时长转换成时间区间
        # 因为有时差，所以获取到最新的是昨天的数据
        # 1个月暂定30天，只影响文件名，具体数据日期是看文件里的真实数据
        day_per_unit = {'d': 1, 'm': 30, 'y': 365}
        days = int(timeframe[:-1]) * day_per_unit[timeframe[-1]]
        yesterday = datetime.datetime.today() - datetime.timedelta(days=1)
        start_day = yesterday - datetime.timedelta(days=days)
        yesterday = yesterday.strftime('%Y%m%d')
        start_day = start_day.strftime('%Y%m%d')

        # 请求数据
        r = requests.post(url, data=submitString, headers=headers)
        df = pd.read_html(r.text)[0]

        # 保存数据
        datafile = '../database/market/%s.csv' % symbol
        info('Writting %s' % datafile)
        df.to_csv(datafile, header=['date', 'open', 'high', 'low', 'close', 'volume'], index=False, encoding='utf-8')
        return datafile


if __name__ == '__main__':
    set_log(INFO)
    spider = Spider_nasdaq()
    timeframe = '10y'
    for symbol in NASDAQ:
        datafile = spider.get_stock_data(symbol, timeframe)
        info(datafile)
