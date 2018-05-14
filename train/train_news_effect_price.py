# -*- coding: utf-8 -*-
# @Author: Kai Zhang
# @Date:   2018-05-04 13:44:30

import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), '..')))

import pandas as pd
import pymongo
import itertools
from concurrent.futures import ProcessPoolExecutor
from sklearn import preprocessing
from sklearn.preprocessing import MinMaxScaler
from sklearn import neighbors
from sklearn.externals import joblib
from common.log import *
from common.config import Config

CONF = Config().data[0]
MONGODB = CONF['MONGODB']
NASDAQ = CONF['NASDAQ']
CRYPTOCURRENCY = CONF['CRYPTOCURRENCY']


class News_effect_price(object):
    """News Effect Price"""

    def __init__(self):
        super(News_effect_price, self).__init__()
        pd.set_option('display.width', 400)
        pd.set_option('display.max_columns', None)
        # pd.set_option('display.max_rows', None)

    def prepare_data(self, symbol):
        """
        预处理数据。
        输入：Symbol，如TSLA等NASDAQ SYMBOL，或BITCOIN，具体值看../conf/conf.yaml
        输出：行情和新闻结合的DataFrame
        """
        self.client = pymongo.MongoClient(MONGODB['IP'], MONGODB['PORT'])
        self.db = self.client['test']
        self.tb = self.db['news_36kr']

        result = self.tb.find({'symbol': symbol})

        if result.count() == 0:
            raise '%s news not in mongodb!'

        '''
        mongodb.test.news_36kr的字段如下：
        {
            "_id" : ObjectId("5af40fca9df9901038bcc784"),
            "news_site" : "36kr",
            "news_id" : 5132875,
            "title" : "马斯克被“打脸”：特斯拉承认还需要融资",
            "publish_time" : "2018-05-08T08:02:14+08:00",
            "full_content" : "",
            "content" : "编者按：本文来自华尔街见闻，作者：文彦；36氪经授权转载上周三特斯拉财报电话会上，CEO马斯克重申|，特
            斯拉未来无须进一步融资，但今天就被自己公司“打脸”。周一上午，特斯拉提交了最新的10-Q报告，并|警告再次融资可能无法避免。报
            告中提到，截止2018年3月31日，特斯拉持有26.7亿美元的现金和现金|，将继续增加产量最终实现每周一万辆的产量，在这个阶段，特斯
            拉预计2018年的总资本支出略低于30亿美元|估我们资本支出需求，并有可能筹集额外资金以支持我们业务的快速增长。”报告指出，特斯
            拉预计，目前的流动",
            "symbol" : "TSLA",
            "keyword" : "特斯拉",
            "sentiment" : 0.8311729431360574,
            "full_sentiment" : 0.9999999999749525
        }
        '''
        news_df = pd.DataFrame(list(result))
        debug(news_df.head())
        debug(news_df.dtypes)
        debug(sys.getsizeof(news_df))

        # 格式化发布日期
        news_df['publish_time'] = news_df['publish_time'].apply(lambda x: pd.Period(x, freq='D'))

        # 计算新闻条数、情感均值
        news_df = news_df.groupby('publish_time').agg({'news_id': ['size'], 'sentiment': ['mean'], 'full_sentiment': ['mean']})
        news_df.columns = ['news_count', 'news_sentiment', 'news_full_sentiment']

        stock_data_file = '../database/market/%s.csv' % symbol
        stock_df = pd.read_csv(stock_data_file)

        # 去掉Nasdaq行情首行的当天行情
        stock_df = stock_df.drop([0])
        stock_df = stock_df.drop(columns=['high', 'low'])

        # 格式化日期，和news_df['publish_time']一致
        stock_df['date'] = stock_df['date'].apply(lambda x: pd.Period(x, freq='D'))

        # 转换字段格式
        stock_df = stock_df.astype(dtype={'volume': 'float64'})

        # 用日期作索引，日期升序排序
        stock_df.set_index(['date'], inplace=True)
        stock_df = stock_df.sort_index(axis=0, ascending=True)

        debug(stock_df.head())
        debug(stock_df.dtypes)

        # 用Merge等同于Join
        prepared_df = pd.merge(stock_df, news_df, how='left', left_index=True, right_index=True)
        prepared_df = prepared_df.loc[:, ['open', 'close', 'volume', 'news_count', 'news_sentiment', 'news_full_sentiment']]

        # 填充缺失值
        prepared_df = prepared_df.fillna(value={'news_count': 0, 'news_sentiment': 0, 'news_full_sentiment': 0})

        # 取较新的数据，比较贴近大形势
        prepared_df = prepared_df.loc['2016-01-01':]
        # prepared_df.reset_index(drop=False, inplace=True)

        debug(prepared_df.head(10))
        debug(prepared_df.tail(10))
        debug(prepared_df.dtypes)
        return prepared_df

    def train_news_effect_close_change(self, symbol, prepared_df):
        """
        【新闻情感的7天移动平均】和【成交量涨跌幅度的7天移动平均】对【收盘价的7天移动平均的涨跌】的影响
        """
        train_name = 'MA_7d_sentiment_and_MA_7d_volumn_pct_change_to_MA_7d_close_rise_or_drop'

        # 计算7天移动平均
        prepared_df['MA_7d_sentiment'] = prepared_df.news_sentiment.rolling(7).mean()
        prepared_df['volumn_pct_change'] = prepared_df.volume.pct_change(1)
        prepared_df['MA_7d_volumn_pct_change'] = prepared_df.volumn_pct_change.rolling(7).mean()
        prepared_df['MA_7d_close'] = prepared_df.close.rolling(7).mean()

        # 计算涨跌，作为分类标签
        prepared_df['label'] = prepared_df['MA_7d_close'].diff(1).shift(-1)
        prepared_df['label'] = prepared_df['label'].apply(lambda x: 'Rise' if x > 0 else 'Drop')

        # 去除空值记录
        prepared_df.dropna(axis=0, how='any', inplace=True)
        debug(prepared_df)
        debug(prepared_df.dtypes)

        label_arr = prepared_df['label'].values
        debug(label_arr)

        # 选取维度
        scaler_df = prepared_df.loc[:, ['MA_7d_sentiment', 'MA_7d_volumn_pct_change']]

        # 归一化
        scaler = MinMaxScaler()
        scaler.fit(scaler_df)
        scaler_arr = scaler.transform(scaler_df)  # 返回的是np.array

        # 分割训练集、验证集、测试集
        training_ratio = 0.5
        validation_ratio = 0.25
        training_count = int(len(scaler_arr) * training_ratio)
        validation_count = int(len(scaler_arr) * validation_ratio)

        training_set = scaler_arr[0:training_count, :]
        training_label = label_arr[0:training_count]

        validation_set = scaler_arr[training_count: training_count + validation_count, :]
        validation_label = label_arr[training_count: training_count + validation_count]

        test_set = scaler_arr[training_count + validation_count:, :]
        test_label = label_arr[training_count + validation_count:]

        # 调优最佳K值
        validation_result = []
        for k in range(1, int(len(scaler_arr) ** 0.5), 1):
            clf = neighbors.KNeighborsClassifier(
                n_neighbors=k,
                weights='uniform', algorithm='auto', leaf_size=1, p=2,
                metric='minkowski', metric_params=None, n_jobs=1
            )
            clf.fit(training_set, training_label)
            score = clf.score(validation_set, validation_label)
            validation_result.append((score, k))

        debug(sorted(validation_result)[::-1][0:10])

        # 最佳K值和得分
        validation_score, k = sorted(validation_result)[::-1][0]

        # 最优训练模型
        clf = neighbors.KNeighborsClassifier(
            n_neighbors=k,
            weights='uniform', algorithm='auto', leaf_size=1, p=2,
            metric='minkowski', metric_params=None, n_jobs=1
        )
        clf.fit(training_set, training_label)

        # 计算测试得分
        test_score = clf.score(test_set, test_label)
        debug(test_score)

        # 持久化模型
        pkl_file_list = joblib.dump(clf, './train_pkl/KNN_%s_K-%d_VALIDATIONSCORE-%f_TESTSCORE-%f_NAME-%s.pkl' % (symbol, k, validation_score, test_score, train_name))
        debug(pkl_file_list)
        return pkl_file_list


if __name__ == '__main__':
    set_log(DEBUG)
    symbol = 'TSLA'
    train = News_effect_price()
    prepared_df = train.prepare_data(symbol)
    pkl_file_list = train.train_news_effect_close_change(symbol, prepared_df)
    info(pkl_file_list)
