# -*- coding: utf-8 -*-
# @Author: Kai Zhang
# @Date:   2018-05-02 14:11:04

import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), '..')))

import yaml

CONF_FILE = '../conf/conf.yaml'


class Config(object):
    """配置类"""

    def __init__(self, yaml_file=CONF_FILE):
        super(Config, self).__init__()
        self.yaml_file = yaml_file
        self.__data = None

    @property
    def data(self):
        # 如果是第一次调用data，读取yaml文档，否则直接返回之前保存的数据
        if not self.__data:
            with open(self.yaml_file, 'rb') as f:
                self.__data = list(yaml.safe_load_all(f))  # load后是个generator，用list组织成列表
        return self.__data


if __name__ == '__main__':
    d = Config().data
    print(d, type(d))
