# -*- coding: utf-8 -*-
# @Author: Kai Zhang
# @Date:   2017-10-25 15:53:13

import requests
import time
import json
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), '..')))
from common.log import *


class Spider36KR(object):
    """36kr.com"""

    def __init__(self):
        super(Spider36KR, self).__init__()
        self.search_url = ''
        self.news_list = []

    def __search_news_from_web(self, search_word, page=1):
        """
        根据关键词search_word和页数标记page，使用36kr XHR API查询新闻。
        探索得知：
            1、此API一次查询条数由per_page参数控制，最多300条。
            2、没有排序参数，默认排序为权重排序，并非发布时间倒序排序
            3、新闻id是唯一且自增的。

        """
        url = 'http://36kr.com/api/search/entity-search?page=%d&per_page=300&keyword=%s&entity_type=post&_=%d' % (page, search_word, int(time.time()))
        debug('准备请求 %s' % url)
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
        r = requests.get(url, headers=headers)
        # r = requests.get(url)
        # debug(r.headers)
        content = r.content
        debug('返回包体长度：%d' % len(content))
        content = json.loads(content)
        converted_news_list = []
        if 'items' not in content['data']:
            return converted_news_list
        news_list = content['data']['items']
        for news in news_list:
            converted_news_list.append(self.__covert_news_formart(news))
        return converted_news_list

    def __search_all_new_news_from_web(self, search_word, last_news_id_from_db):
        has_new_news = True
        page = 1
        new_news_list = []
        while has_new_news:
            news_list = self.__search_news_from_web(search_word, page)
            debug('len(news_list) = %d' % len(news_list))
            if len(news_list) == 0:
                break
            for news in news_list:
                if news['news_id'] > last_news_id_from_db:
                    new_news_list.append(news)
            debug('len(new_news_list) = %d' % len(new_news_list))
            self.__insert_news_list_to_db(search_word, new_news_list)
            new_news_list = []
            page += 1
        return new_news_list

    def __covert_news_formart(self, news):
        """
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
            news_site
            news_id
            title
            publish_time
            content
        """
        # debug(str(news).encode('utf-8'))
        converted_news = {}
        converted_news['news_site'] = '36kr'
        converted_news['news_id'] = news['id']
        converted_news['title'] = news['title']
        converted_news['publish_time'] = news['published_at']
        if 'content' in news['highlight']:
            converted_news['content'] = '|'.join(c for c in news['highlight']['content'])
        else:
            converted_news['content'] = news['highlight']['title']
        return converted_news

    def __insert_news_list_to_db(self, search_word, news_list):
        f = open('36kr_%s.txt' % search_word, 'a+', encoding='utf-8')
        for news in news_list:
            f.write(str(news))
            f.write('\n')
        f.close()
        return True

    def __get_last_news_from_db(self, search_word):
        db_file = '36kr_%s.txt' % search_word
        if not os.path.exists(db_file):
            last_news_id_from_db = 0
            return last_news_id_from_db
        f = open('36kr_%s.txt' % search_word, 'r', encoding='utf-8')
        db_news_list = f.read().split('\n')
        f.close()

        # json.loads无法解析单引号，只能用eval了。
        db_news_id_list = []
        for news in db_news_list[0:]:
            if news == '':
                continue
            news = eval(news)
            db_news_id_list.append(news['news_id'])
        if len(db_news_id_list) > 0:
            last_news_id_from_db = max(db_news_id_list)
        else:
            last_news_id_from_db = 0
        return last_news_id_from_db

    def update_news_by_keyword(self, keyword):
        last_news_id_from_db = self.__get_last_news_from_db(keyword)
        debug('last_news_id_from_db = %s' % last_news_id_from_db)
        # new_news_list = self.__search_all_new_news_from_web(keyword, last_news_id_from_db)
        # self.__insert_news_list_to_db(keyword, new_news_list)
        self.__search_all_new_news_from_web(keyword, last_news_id_from_db)
        return True


if __name__ == '__main__':
    set_log(DEBUG)
    site = Spider36KR()
    keyword_list = ['百度', '网易', '迅雷', '区块链', 'EOS']
    for keyword in keyword_list[0:]:
        site.update_news_by_keyword(keyword)
