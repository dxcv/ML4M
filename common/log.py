# -*- coding: utf-8 -*-
# @Author: Kai Zhang
# @Date:   2016-07-21 15:29:42


import logging
from logging import *

LOG_LEVEL = logging.DEBUG


def set_log_file(log_level, log_file='test.log'):
    logging.basicConfig(level=log_level,
                        format='%(asctime)s %(filename)s[line:%(lineno)d] [%(levelname)s] %(message)s',
                        # datefmt='%a %d %b %Y %H:%M:%S',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        filename=log_file,
                        filemode='a')


# 定义一个StreamHandler，将INFO级别或更高的日志信息打印到标准错误，并将其添加到当前的日志处理对象
def set_console(log_level):
    console = logging.StreamHandler()
    console.setLevel(LOG_LEVEL)
    formatter = logging.Formatter(
        '%(asctime)s %(filename)s[line:%(lineno)d] [%(levelname)s] %(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)


def set_log(log_level=logging.DEBUG, log_file='test.log'):
    set_log_file(log_level, log_file)
    set_console(log_level)
