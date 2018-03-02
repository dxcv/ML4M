# -*- coding: utf-8 -*-
# @Author: Kai Zhang
# @Date:   2018-02-27 10:55:40

import os
import pandas as pd
import time
import requests
import seaborn as sn
import matplotlib.pyplot as plt
# import datetime
import numpy as np

coin = 'eos'
start_date = '20170101'
end_date = time.strftime('%Y%m%d')
url = 'https://coinmarketcap.com/currencies/%s/historical-data/?start=%s&end=%s' % (coin, start_date, end_date)

if os.path.isfile('coinmarketcap_%s_%s_%s.html' % (coin, start_date, end_date)):
    print('data file exists')
    pass
else:
    r = requests.get(url)
    with open('coinmarketcap_%s_%s_%s.html' % (coin, start_date, end_date), 'w', encoding='utf-8') as f:
        f.write(r.text)

# get market info for bitcoin from the start of 2016 to the current day
# bitcoin_market_info = pd.read_html("https://coinmarketcap.com/currencies/bitcoin/historical-data/?start=20130428&end="+time.strftime("%Y%m%d"))[0]
# bitcoin_market_info = pd.read_html("https://coinmarketcap.com/currencies/eos/historical-data/?start=20180210&end="+time.strftime("%Y%m%d"))[0]
bitcoin_market_info = pd.read_html('coinmarketcap_%s_%s_%s.html' % (coin, start_date, end_date))[0]
# print(type(bitcoin_market_info))
# print(bitcoin_market_info)

# convert the date string to the correct date format
bitcoin_market_info = bitcoin_market_info.assign(Date=pd.to_datetime(bitcoin_market_info['Date']))
# print(bitcoin_market_info)

# when Volume is equal to '-' convert it to 0
# print(type(bitcoin_market_info.loc[bitcoin_market_info['Market Cap'] == '-']))
# print(bitcoin_market_info.loc[bitcoin_market_info['Market Cap'] == '-'])
# print(type(bitcoin_market_info.loc[bitcoin_market_info['Market Cap'] == '-', 'Market Cap']))
# print(bitcoin_market_info.loc[bitcoin_market_info['Market Cap'] == '-', 'Market Cap'])
try:
    bitcoin_market_info.loc[bitcoin_market_info['Volume'] == '-', 'Volume'] = 0
except Exception as e:
    # raise e
    # print(e)
    pass

try:
    bitcoin_market_info.loc[bitcoin_market_info['Market Cap'] == '-', 'Market Cap'] = 0
except Exception as e:
    # raise e
    # print(e)
    pass

# convert to int
bitcoin_market_info['Volume'] = bitcoin_market_info['Volume'].astype('int64')
bitcoin_market_info['Market Cap'] = bitcoin_market_info['Market Cap'].astype('int64')

# look at the first few rows
print(bitcoin_market_info.head())
print(bitcoin_market_info.tail())

# draw data pic
t1 = bitcoin_market_info.plot(kind='line', x='Date', y='Close')
t2 = bitcoin_market_info.plot(kind='bar', x='Date', y='Close')
print(t1)
print(t2)
# plt.show()
