# -*- coding: utf-8 -*-
# @Author: Kai Zhang
# @Date:   2017-10-31 16:45:30


import pymongo
from pymongo import MongoClient
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), '..')))
from common.log import *


class NewsDB(object):
    """News Database"""

    def __init__(self, db_type):
        super(NewsDB, self).__init__()
        self.db_type = db_type
        self.client = None
        self.db = None
        self.conn()

    def conn(self):
        self.client = MongoClient('mongodb://127.0.0.1:27017')
        self.db = self.client.test
        pass

    def read(self):
        self.cursor = self.db.news_36kr.find()
        for document in self.cursor:
            debug(document)
        pass

    def write(self):
        pass

    def close(self):
        pass


if __name__ == '__main__':
    set_log(DEBUG)
    db = NewsDB('mongodb')
    db.read()
