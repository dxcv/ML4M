# -*- coding: utf-8 -*-
# @Author: Kai Zhang
# @Date:   2017-10-31 16:45:30


import pymongo
from pymongo import MongoClient
import os
import sys
import time
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), '..')))
from common.log import *


def get_db(db_type, database, table):
    if db_type == 'mongodb':
        return MongoDB(database, table)
    elif db_type == 'textfile':
        return TextfileDB(database, table)
    else:
        raise Exception('ML4M Value Error', 'db_type=%s is illegal' % db_type)


class MongoDB(object):
    """Mongodb Database"""

    def __init__(self, database, table):
        super(MongoDB, self).__init__()
        self.database = database
        self.table = table
        self.tb = None
        self.conn()

    def conn(self):
        client = MongoClient('mongodb://127.0.0.1:27017')
        db = client[self.database]
        self.tb = db[self.table]

    def read(self, filter):
        cursor = self.tb.find_one()
        # return cursor.document
        return cursor

    def write(self, data):
        document_id = self.tb.insert_one(data).inserted_id
        return document_id

    def close(self):
        return True


class TextfileDB(object):
    """Text file databases"""

    def __init__(self, database, table):
        super(TextfileDB, self).__init__()
        self.database = database
        self.table = table
        self.tb = None
        self.conn()

    def conn(self):
        filepath = os.path.join(self.database, self.table)
        self.tb = open(filepath, 'a+', encoding='utf-8')

    def read(self, filter):
        return self.tb.readline()

    def write(self, data):
        return self.tb.write(str(data) + '\n')

    def close(self):
        return self.tb.close()

    def __del__(self):
        self.close()


if __name__ == '__main__':
    set_log(DEBUG)
    news = {'news_site': '36kr',
            'keyword': '测试',
            'news_id': '0',
            'title': '测试',
            'publish_time': int(time.time()),
            'content': '测试第一段|测试第二段',
            'full_content': '测试全文',
            }
    db = get_db('mongodb', 'test', 'news_36kr')
    print(db)
    print(db.read(''))
    print(db.write(news))

    db = get_db('textfile', '../database/', '36kr_EOS.txt')
    print(db)
    print(db.read(''))
    print(db.write(news))
