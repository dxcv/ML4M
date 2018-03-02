# -*- coding: utf-8 -*-
# @Author: Kai Zhang
# @Date:   2017-11-22 16:57:17

import sys
import os
import pandas as pd
# import time
# import requests
# import seaborn as sn
# import matplotlib.pyplot as plt
import numpy as np

# print(sys.path)
# print(os.path.dirname(os.path.abspath('__file__')))
# print(os.path.abspath(os.path.join(os.getcwd(), '..')))

d = {'col1': [1, -2], 'col2': [3, 4]}
df = pd.DataFrame(data=d)
print(df)
# print(df.abs())
df2 = pd.DataFrame([(100, 200), (300, 400)], columns=['a', 'b'])
print(df.add(df2, fill_value=0))
# df2 = pd.DataFrame(np.random.randint(low=0, high=10, size=(5, 5)), columns=['a', 'b', 'c', 'd', 'e'])
# print(df2)
