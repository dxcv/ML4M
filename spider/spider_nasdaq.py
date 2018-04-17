# -*- coding: utf-8 -*-
# @Author: Kai Zhang
# @Date:   2018-04-10 14:17:40


import os
import sys
import time
import datetime
import requests
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), '..')))
from common.log import *


class Spider_nasdaq(object):
    """Nasdaq股数据的爬虫类"""

    def __init__(self):
        pass

    def get_stock_data(self, stock, timeframe, save_csv='false'):
        """
        Desc:
            通过Nasdaq官网获取Nasdaq股票数据，保存为HTML文件。
            最好一次性获取10年（10y）数据，因为数据并不大。
        Parameter:
            stock: 字符串，股票代码
            timeframe: 时间区间，只能按照Nasdaq官网的规定，如5d、1m、3m、6m、18m、1y、2y...10y。
            save_csv: 暂时无用！字符串，true或false，是否获取Nasdaq提供的csv数据文件。
        Return:
            无。
        原理:
            通过Nasdaq官网获取Nasdaq股票数据。
        """

        # 构造参数
        url = 'https://www.nasdaq.com/symbol/tsla/historical'
        payload = '%s|%s|%s' % (timeframe, stock, save_csv)
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
        datafile = '../database/nasdaq_%s_%s_%s.html' % (stock, start_day, yesterday)

        # 判断数据文件是否已经存在，不存在就下载保存数据
        if os.path.isfile(datafile):
            debug('datafile exists')
            return False
        else:
            r = requests.post(url, data=payload, headers=headers)
            with open(datafile, 'w', encoding='utf-8') as f:
                f.write(r.text)
        return True


if __name__ == '__main__':
    set_log(INFO)
    stock = 'TSLA'
    timeframe = '10y'
    spider = Spider_nasdaq()
    spider.get_stock_data(stock, timeframe)
