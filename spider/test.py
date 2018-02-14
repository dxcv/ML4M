# -*- coding: utf-8 -*-
# @Author: Kai Zhang
# @Date:   2017-11-22 16:57:17

import sys
import os
print(sys.path)
print(os.path.dirname(os.path.abspath('__file__')))
print(os.path.abspath(os.path.join(os.getcwd(), '..')))
