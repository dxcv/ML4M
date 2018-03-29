# -*- coding: utf-8 -*-
# @Author: Kai Zhang
# @Date:   2018-02-27 10:55:40

import os
import pandas as pd
import pandas
import time
import requests
import seaborn as sn
import matplotlib.pyplot as plt
# import datetime
import numpy as np
import json

# pandas.set_option('display.column_space', 10000)
# pandas.set_option('display.line_width', 10000)
# pandas.set_option('display.max_columns', 10000)
# print(pandas.get_option('display.column_space'))
# print(pandas.get_option('display.line_width'))
# print(pandas.get_option('display.max_columns'))


coin = 'eos'
start_date = '20170101'
end_date = time.strftime('%Y%m%d')
url = 'https://coinmarketcap.com/currencies/%s/historical-data/?start=%s&end=%s' % (coin, start_date, end_date)

if os.path.isfile('../database/coinmarketcap_%s_%s_%s.html' % (coin, start_date, end_date)):
    print('data file exists')
    pass
else:
    r = requests.get(url)
    with open('../database/coinmarketcap_%s_%s_%s.html' % (coin, start_date, end_date), 'w', encoding='utf-8') as f:
        f.write(r.text)

# get market info for bitcoin from the start of 2016 to the current day
# bitcoin_market_info = pd.read_html("https://coinmarketcap.com/currencies/bitcoin/historical-data/?start=20130428&end="+time.strftime("%Y%m%d"))[0]
# bitcoin_market_info = pd.read_html("https://coinmarketcap.com/currencies/eos/historical-data/?start=20180210&end="+time.strftime("%Y%m%d"))[0]
bitcoin_market_info = pd.read_html('../database/coinmarketcap_%s_%s_%s.html' % (coin, start_date, end_date))[0]
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
# print(bitcoin_market_info.head())
# print(bitcoin_market_info.tail())

# draw data pic
t1 = bitcoin_market_info.plot(kind='line', x='Date', y='Close')
t2 = bitcoin_market_info.plot(kind='bar', x='Date', y='Close')
# print(t1)
# print(t2)
# plt.show()

# print('bitcoin_market_info.index', bitcoin_market_info.index)
# print('bitcoin_market_info.columns', bitcoin_market_info.columns)
# print('bitcoin_market_info.values', bitcoin_market_info.values)
# print('bitcoin_market_info.describe()', bitcoin_market_info.describe())
# print('bitcoin_market_info.T', bitcoin_market_info.T)
# print(bitcoin_market_info.sort_index(axis=1, ascending=True))
# print(bitcoin_market_info.sort_values(by='Volume'))
# print(bitcoin_market_info['Close'].head())
# print(bitcoin_market_info[0:10])
# print(bitcoin_market_info['2018-03-01':'2018-03-05'])
# bitcoin_market_info.index = bitcoin_market_info['Date']
bitcoin_market_info['Date'] = bitcoin_market_info['Date'].apply(lambda x: pd.Period(x, freq='D'))
bitcoin_market_info.set_index(['Date'], inplace=True)
bitcoin_market_info = bitcoin_market_info.sort_index(axis=0, ascending=True)
# print(bitcoin_market_info.index)
# print(bitcoin_market_info['20180304':'20180301'])
# print(bitcoin_market_info[0:10])
# print(bitcoin_market_info.loc['2018-03-01':'2018-03-05'])
# print(bitcoin_market_info.loc['2018-03-01':'2018-03-05', ['Open', 'Close']])
# print(bitcoin_market_info.loc['2018-03-04', ['Open', 'Close']])
# print(bitcoin_market_info.iloc[-1])
# print(bitcoin_market_info.iloc[[0, -1, -2], [0, 1]])
# print(bitcoin_market_info[bitcoin_market_info.Close > 10])
# print(bitcoin_market_info[bitcoin_market_info > 0])
# bitcoin_market_info['News'] = 0
# print(type(bitcoin_market_info.iloc[0]))
# bitcoin_market_info.loc['2018-03-01', 'News'] = 100
# print(bitcoin_market_info.head())
# print(bitcoin_market_info.tail())
# print(bitcoin_market_info.mean())

s = {'news_site': '36kr', 'news_id': 5116563, 'title': '2017杀入风投领域的46家新VC们，20%与区块链相关', 'publish_time': '2018-01-29T16:06:06+08:00', 'content': ' INBlockchain」，是一家区块链行业的投资与孵化平台，前后投资 Steem，EOS，Sia'}
s = [{'news_site': '36kr', 'news_id': 5116563}, {'news_site': '36kr', 'news_id': 5116564}]
# print(json.dumps(s))
# s = [{"col 1":"a","col 2":"b"}, {"col 1":"c","col 2":"d"}]
n = []
# with open('../database/36kr_EOS.txt', 'r', encoding='utf-8') as f:
    # for line in f:
        # print(line, end='')
        # print(line)
        # print(type(line))
        # l = f.readline()
        # print(l)
        # l = l.strip()
        # n.append(eval(line))
# print(len(n))
# s = "{'news_site': '36kr', 'news_id': 5116563}"
# print(json.loads(s.replace('\'', '\"')))
# print(json.dumps(s))
with open('../database/36kr_EOS.txt', 'r', encoding='utf-8') as f:
    news = pandas.read_json(f.read(), orient='records', typ='frame')
# print(news)
# print(len(news))
# print(news.loc[:, ['news_id', 'publish_time', 'title']])

# bitcoin_market_info = bitcoin_market_info + news
# print(bitcoin_market_info.head())

print(pd.Period('2018-01-30T09:15:17+08:00', freq='D'))
# news = news.loc[:, ['news_id', 'publish_time']]
news = news.loc[:, ['publish_time', 'news_id', 'content']]
news['publish_time'] = news['publish_time'].apply(lambda x: pd.Period(x, freq='D'))
# pd.Period(news['publish_time'], freq='D')
# lambda x: map(int, df[(df[0]==x[0])&(df[1]==x[1])].count() <= 1), axis=1
print(news.head())

# news = news.groupby('publish_time').size().reset_index(name='counts')
# news = news.groupby('publish_time').agg({
#     'news_id': ['median', 'min', 'count'],
# })
news = news.groupby('publish_time').size().to_frame(name='counts')
news['counts'] = news['counts'].astype('int64')
print(news)
print(news.dtypes)

news = news.sort_index(axis=0, ascending=True)
print(news.head())

# bitcoin_market_info.combine(news)
# print(bitcoin_market_info.head())
# bc = pd.merge(bitcoin_market_info, news, how='left', left_index=True, right_index=True)
bc = bitcoin_market_info.join(news)
# bc['counts'] = bc['counts'].astype('int64')
print(bc.dtypes)
# print(bc.head())
# print(bc.loc[pd.Timestamp('20180201'):pd.Timestamp('20180301')])

# 用Merge等同于Join
bc = pd.merge(bitcoin_market_info, news, how='left', left_index=True, right_index=True)
bc = bc.loc[pd.Timestamp('20180201'):pd.Timestamp('20180301'), ['Close', 'Volume', 'counts']]
bc = bc.fillna(value={'counts': 0})
print(bc)
