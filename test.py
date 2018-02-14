# -*- coding: utf-8 -*-
# @Author: Kai Zhang
# @Date:   2017-11-01 16:42:28

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath('__file__')))
from common.log import *
from spider.spider_36kr import Spider36KR

if __name__ == '__main__':
    set_log(DEBUG)
    spider = Spider36KR()
    debug(spider)
