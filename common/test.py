# -*- coding: utf-8 -*-
# @Author: Kai Zhang
# @Date:   2018-03-01 08:48:31

import os

class DB(object):
    """docstring for DB"""
    def __init__(self, type):
        super(DB, self).__init__()
        self.type = type
        # if type == 'm':
        #     return DBM
    
    def run(self):
        print('DB run')
        pass


class DBM(object):
    """docstring for DBM"""
    def __init__(self, type):
        super(DBM, self).__init__()
        self.type = type
    
    def run(self):
        print('DBM run')
        pass


DBD = DBM
print(DBM)
print(type(DBM))


if __name__ == '__main__':
    db = DB('m')
    db.run()
    db2 = DBD('m')
    db2.run()
    d = os.path.join('../database/', '36kr_EOS.txt')
    print(d)
