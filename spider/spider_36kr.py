# -*- coding: utf-8 -*-
# @Author: Kai Zhang
# @Date:   2018-05-02 14:31:49

import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), '..')))

import requests
import time
import json
import re
from snownlp import SnowNLP
import pymongo
from common.log import *
from common.config import Config

CONF = Config().data[0]
MONGODB = CONF['MONGODB']
NASDAQ = CONF['NASDAQ']
CRYPTOCURRENCY = CONF['CRYPTOCURRENCY']


class Spider36KR(object):
    """36kr.com"""

    def __init__(self):
        super(Spider36KR, self).__init__()

        # 初始化和连接数据库
        self.client = pymongo.MongoClient(MONGODB['IP'], MONGODB['PORT'])
        self.db = self.client['test']
        self.__init_mongodb()
        self.tb = self.db['news_36kr']

    def __init_mongodb(self):
        """
        初始化MongoDB
        """
        # 尝试新建表
        # 如果表已经存在，create_collection会抛异常
        try:
            self.db.create_collection('news_36kr')
        except Exception as e:
            # raise e
            info(e)

        # 尝试新建索引
        # 如果相同的索引已经储存，create_index并不会重复新建，也不会抛异常
        try:
            self.tb = self.db['news_36kr']
            self.tb.create_index(keys=[('news_id', pymongo.DESCENDING), ('symbol', pymongo.TEXT)], unique=True)
        except Exception as e:
            # raise e
            error(e)

    def get_news_per_page(self, search_word, page=1):
        """
        根据关键词search_word和页数索引page，使用36kr XHR API查询新闻。
        探索得知：
            1、此API一次查询条数由per_page参数控制，最多300条。
            2、没有排序参数，默认排序为权重排序，并非发布时间倒序排序
            3、新闻id是唯一且自增的。

        """
        url = 'http://36kr.com/api/search/entity-search'

        # 构造GET参数
        params = {
            'page': page,
            'per_page': 300,
            'keyword': search_word,
            'entity_type': 'post',
            '_': int(time.time()),
        }

        # 伪造请求头
        headers = {
            # 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            # 'Accept-Encoding': 'gzip, deflate',
            # 'Accept-Language': 'zh-CN,zh;q=0.9',
            # 'Cache-Control': 'max-age=0',
            # 'Connection': 'keep-alive',
            # 'Cookie': '',
            # 'Host': '36kr.com',
            # 'Referer': 'http://36kr.com/',
            # 'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36',
        }

        r = requests.get(url, params=params, headers=headers)
        info('准备请求 %s' % r.url)
        content = r.content
        debug('返回包体长度：%d' % len(content))

        content = json.loads(content)

        # 没有item参数，说明翻页超底了，没有新闻返回
        if 'items' not in content['data']:
            return []
        else:
            return content['data']['items']

    def get_all_news(self, symbol, search_word):
        """
        抓取所有新的新闻。
        输入：股票代码、搜索关键词
        返回：插入数据库的新的新闻数目
        """
        page = 1
        news_count = 0
        new_news_count = 0
        insert_count = 0

        info('Getting news, symbol = %s, keyword = %s' % (symbol, search_word))

        while True:
            # 逐页请求XHR API，获取新闻
            news_list = self.get_news_per_page(search_word, page)
            page += 1
            debug('len(news_list) = %d' % len(news_list))

            # 接口没返回新闻，说明翻页超底了
            if len(news_list) == 0:
                break

            for news in news_list:
                news_count += 1
                # 如果news_id-keyword已经存在数据库了，则无需处理
                # 如果一定要处理，那在插入mongodb时，会被索引unique所限制，最终抛出异常
                if self.tb.find_one({'news_id': news['id'], 'symbol': symbol}) is not None:
                    continue
                new_news_count += 1

                # 处理并插入新闻数据的字段
                news = self.format_news(news)
                news['symbol'] = symbol
                news['keyword'] = search_word
                document_id = self.insert_news_to_db(news)
                if document_id:
                    insert_count += 1

        info('news_count = %d, new_news_count=%d, insert_count=%d' % (news_count, new_news_count, insert_count))
        return insert_count

    def format_news(self, news):
        """
        去除新闻数据的无用字段，保留或转换成目标字段。
        原字段：
            {
                id: 5099726,
                title: "百度三季报：百度外卖卖了42亿，信息流年化收入预计超10亿美元",
                project_id: "1",
                views_count: "0",
                mobile_views_count: "0",
                app_views_count: "0",
                monographic_id: 0,
                domain_id: "0",
                goods_id: "0",
                is_tovc: "0",
                is_free: "0",
                cover: "https://pic.36krcnd.com/201710/27053126/5ygpaw8k4ygfl3ox!heading",
                template_info: {
                template_type: "small_image",
                template_title: "百度三季报：百度外卖卖了42亿，信息流年化收入预计超10亿美元",
                template_title_isSame: true,
                template_cover: [
                "https://pic.36krcnd.com/201710/27053126/5ygpaw8k4ygfl3ox!heading"
                ]
                },
                published_at: "2017-10-27T13:52:53+08:00",
                column_name: "大公司",
                column_id: "23",
                highlight: {
                content: [
                "百度（NASDAQ：BIDU）交出了2017年第三季度财报。本季度百度营收为235亿人民币（约合",
                "准则下，百度净利润91亿元人民币（约合13.6亿美元），同比增长163%；其中移动营收占比73%，高",
                "于去年同期的64%。百度预计，基于三季度业绩百度信息流业务预期年化收入将超过10亿美元。尽管第三季度",
                "净利润超出市场预期，但百度第三季度营收及第四季度营收预期未达市场预期，受此影响，百度股价在周四的盘后",
                "以及人工智能商业化是百度本季度财报中的亮点。8月24日，饿了么宣布合并百度外卖。合并完成后，百度外卖"
                ],
                title: [
                "<em>百</em><em>度</em>三季报：<em>百</em><em>度</em>外卖卖了42亿，信息流年化收入预计超10亿美元"
                ],
                content_light: [
                "<em>百</em><em>度</em>（NASDAQ：BIDU）交出了2017年第三季度财报。本季度<em>百</em><em>度</em>营收为235亿人民币（约合",
                "准则下，<em>百</em><em>度</em>净利润91亿元人民币（约合13.6亿美元），同比增长163%；其中移动营收占比73%，高",
                "于去年同期的64%。<em>百</em><em>度</em>预计，基于三季度业绩<em>百</em><em>度</em>信息流业务预期年化收入将超过10亿美元。尽管第三季度",
                "净利润超出市场预期，但<em>百</em><em>度</em>第三季度营收及第四季度营收预期未达市场预期，受此影响，<em>百</em><em>度</em>股价在周四的盘后",
                "以及人工智能商业化是<em>百</em><em>度</em>本季度财报中的亮点。8月24日，饿了么宣布合并<em>百</em><em>度</em>外卖。合并完成后，<em>百</em><em>度</em>外卖"
                ]
                },
                _type: "post",
                _score: 18.295677
            },
        目标字段：
            news_site：新闻站点，这里固定是36kr
            news_id：新闻id
            title：新闻标题
            publish_time：发布时间
            content：亮点摘要汇总
            full_content：新闻全文，无法从接口获取，必须再抓取文章页，暂时为空
        """

        # 过滤特殊字符
        # 蛋疼的是，Unicode有很多奇葩字符，不可视，不被聚类，得单独过滤
        # 比如\u200b，U+200B: ZERO WIDTH SPACE
        # 在Python和Unicode character property中，U+200B不被认为是空格字符，因此不会被\s或strip过滤
        # 参考：https://bugs.python.org/issue13391
        filter_char = '[\s\"\'\{\}\[\]\(\)]'
        formatted_news = {}
        formatted_news['news_site'] = '36kr'
        formatted_news['news_id'] = news['id']
        formatted_news['title'] = re.sub(filter_char, '', news['title'])
        formatted_news['publish_time'] = re.sub(filter_char, '', news['published_at'])
        formatted_news['full_content'] = ''

        # 合并亮点摘要
        if 'content' in news['highlight']:
            formatted_news['content'] = '|'.join(c for c in news['highlight']['content'])
        else:
            # 没有['highlight']['content']，则用['highlight']['title'][0]
            formatted_news['content'] = news['highlight']['title'][0]
            # 简单粗暴用尖括号判断过滤HTML标签
            formatted_news['content'] = re.sub(r'<.*?>', '', formatted_news['content'])
            formatted_news['content'] = re.sub(filter_char, '', formatted_news['content'])

        return formatted_news

    def insert_news_to_db(self, news):
        """
        把新闻写入MongoDB数据库
        """
        try:
            document_id = self.tb.insert_one(news).inserted_id
        except Exception as e:
            document_id = None
            raise e
        return document_id

    def calc_sentiment(self, text):
        """
        计算情感得分。
        """
        return SnowNLP(text).sentiments

    def refresh_sentiment(self, symbol, search_word, refresh_all=False):
        """
        慎用！！！
        耗时较大。
        除非是更换情感得分计算算法。
        刷新全部新闻的情感得分。
        已有得分的，重新计算并写入。
        没有得分或得分为空的，重新计算并写入。

        refresh_all: True，全部重新计算；False，只计算缺少情感得分的。
        """
        info('Refreshing sentiment, keyword = %s' % search_word)
        refresh_count = 0

        # 读取原先的新闻数据
        if refresh_all:
            news_list = self.tb.find({})
        else:
            news_list = self.tb.find({'sentiment': {'$exists': False}, 'symbol': symbol, 'keyword': search_word})

        # 计算并更新情感得分
        for news in news_list[:]:
            sentiment = self.calc_sentiment(news['title'])
            full_sentiment = self.calc_sentiment(news['content'])
            debug('news_id=%d, title=%s, sentiment=%f, full_sentiment=%f' % (news['news_id'], news['title'], sentiment, full_sentiment))
            result = self.tb.update_one(
                {'news_id': news['news_id'], 'keyword': search_word},
                {'$set': {'sentiment': sentiment, 'full_sentiment': full_sentiment}}
            )
            debug(result.raw_result)
            refresh_count += 1

        info('refresh_sentiment_count = %d' % refresh_count)
        return refresh_count


if __name__ == '__main__':
    set_log(INFO)
    spider = Spider36KR()
    symbol_dict = dict(NASDAQ, **CRYPTOCURRENCY)
    for symbol, keyword_list in symbol_dict.items():
        for keyword in keyword_list:
            info('Getting %s %s' % (symbol, keyword))
            spider.get_all_news(symbol, keyword)
            spider.refresh_sentiment(symbol, keyword, refresh_all=False)
