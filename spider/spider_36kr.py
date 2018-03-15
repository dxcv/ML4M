# -*- coding: utf-8 -*-
# @Author: Kai Zhang
# @Date:   2017-10-25 15:53:13

import requests
import time
import json
import os
import sys
import re
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
        根据关键词search_word和页数索引page，使用36kr XHR API查询新闻。
        探索得知：
            1、此API一次查询条数由per_page参数控制，最多300条。
            2、没有排序参数，默认排序为权重排序，并非发布时间倒序排序
            3、新闻id是唯一且自增的。

        """
        url = 'http://36kr.com/api/search/entity-search?page=%d&per_page=300&keyword=%s&entity_type=post&_=%d' % (page, search_word, int(time.time()))
        debug('准备请求 %s' % url)

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

        r = requests.get(url, headers=headers)
        content = r.content
        debug('返回包体长度：%d' % len(content))
        content = json.loads(content)

        # 没有item参数，说明翻页超底了，没有新闻返回
        if 'items' not in content['data']:
            return []
        else:
            return content['data']['items']

    def __search_all_new_news_from_web(self, search_word, last_news_id_from_db):
        """
        抓取所有新的新闻。
        输入：搜索词，数据库中最大的news_id
        返回：新的新闻的list
        """
        page = 1
        new_news_list = []
        while True:
            # 逐页请求XHR API，获取新闻
            news_list = self.__search_news_from_web(search_word, page)
            page += 1
            debug('len(news_list) = %d' % len(news_list))

            # 接口没返回新闻，说明翻页超底了
            if len(news_list) == 0:
                break

            # 判断新闻是否是新的，通过news_id是否大于数据库中最大的news_id判断
            for news in news_list:
                # debug(news)
                if news['id'] > last_news_id_from_db:
                    # 处理并插入新闻数据的字段
                    new_news_list.append(self.__covert_news_formart(news))

        debug('len(new_news_list) = %d' % len(new_news_list))
        return new_news_list

    def __covert_news_formart(self, news):
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
        converted_news = {}
        converted_news['news_site'] = '36kr'
        converted_news['news_id'] = news['id']
        converted_news['title'] = re.sub(filter_char, '', news['title'])
        converted_news['publish_time'] = re.sub(filter_char, '', news['published_at'])
        converted_news['full_content'] = ''

        # 合并亮点摘要
        if 'content' in news['highlight']:
            converted_news['content'] = '|'.join(c for c in news['highlight']['content'])
        else:
            converted_news['content'] = news['highlight']['title']
        converted_news['content'] = re.sub(filter_char, '', converted_news['content'])

        return converted_news

    def __insert_news_list_to_db(self, search_word, news_list):
        """
        把新的新闻插入数据库，目前只能以JSON String形式保存在文本文件
        TODO: 实现可选存入文本或MongoDB
        """
        # 文本文件路径
        txtfile = '../database/36kr_%s.txt' % search_word

        # 读取原先的新闻数据
        # 若文件不存在，或内容为空导致JSON解析失败，依然可以赋值空列表
        # TODO: 增强容错性，保护原数据
        all_news_list = []
        if os.path.exists(txtfile):
            with open(txtfile, 'r', encoding='utf-8') as f:
                all_news_list = json.loads(f.read())

        # 合并原新闻和本次抓取的新的新闻
        all_news_list = all_news_list + news_list

        # 转换为JSON字符串，覆盖写入文本文件
        # TODO: 增强容错性，保护原数据
        try:
            news = json.dumps(all_news_list)
            f = open(txtfile, 'w+', encoding='utf-8')
            f.write(news)
        except Exception as e:
            # raise e
            return False
        finally:
            f.close()

        return True

    def __get_last_news_from_db(self, search_word):
        """
        获取数据库中最大的新闻ID
        """
        txtfile = '../database/36kr_%s.txt' % search_word
        last_news_id_from_db = 0
        news_list = []

        # 文本文件不存在，说明之前从未抓取过数据，直接返回0
        if not os.path.exists(txtfile):
            return last_news_id_from_db

        with open(txtfile, 'r', encoding='utf-8') as f:
            news_list = json.loads(f.read())

        # 计算news_id最大值
        last_news_id_from_db = max(news['news_id'] for news in news_list)
        # last_news_id_from_db = max(dict_list, key=lambda x: x['news_id'])
        return last_news_id_from_db

    def update_news_by_keyword(self, keyword):

        # 从数据库获取已有新闻的最大ID
        last_news_id_from_db = self.__get_last_news_from_db(keyword)
        debug('last_news_id_from_db = %s' % last_news_id_from_db)

        # 抓取新的新闻，即news_id大于原有最大news_id的
        new_news_list = self.__search_all_new_news_from_web(keyword, last_news_id_from_db)

        # 把新的新闻存入数据库
        ret = self.__insert_news_list_to_db(keyword, new_news_list)
        return ret


if __name__ == '__main__':
    set_log(DEBUG)
    site = Spider36KR()
    keyword_list = ['百度', '网易', '迅雷', '区块链', 'EOS']
    keyword_list = ['EOS']
    for keyword in keyword_list[0:]:
        site.update_news_by_keyword(keyword)
