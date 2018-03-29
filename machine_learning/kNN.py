# -*- coding: utf-8 -*-
# @Author: Kai Zhang
# @Date:   2018-03-21 16:13:32

import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sn
import sys
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), '..')))
from common.log import *


class kNN(object):
    """
    kNN算法实现类
    """

    def __init__(self):
        super(kNN, self).__init__()

    @staticmethod
    def classify0(inX, dataSet, k):
        """
        Desc：
            K近邻距离计算和预测值的实现
        Parameter：
            inX：目标数据点，pandas.DataFrame类型，和dataSet结构一致
            dataSet：数据集，pandas.DataFrame类型，和inX结构一致
            k：K近邻的K值
        Return：
            inX_lable：目标数据点inX的类别预测值

        原理：
            对于每一个在数据集中的数据点：
                计算目标的数据点（需要分类的数据点）与该数据点的距离
                将距离排序：从小到大
                选取前K个最短距离
                选取这K个中最多的分类类别
                返回该类别来作为目标数据点的预测值
        """

        # 合并方式创建目标数据点的矩阵
        diffMat = pd.concat([inX] * len(dataSet), ignore_index=True)
        debug(pd.Timestamp('now'))

        # 矩阵求差
        # 因为dataSet和diffMat结构相同，带index，但index数据不同，所以不能直接相减
        # 这里要保留dataSet的真实index，因此去减diffMat.value
        diffMat = dataSet - diffMat.values
        debug(pd.Timestamp('now'))

        # 计算目标数据点和数据集中的每一个点的欧式距离，存在distant字段里
        diffMat['distant'] = diffMat.apply(
            lambda x: np.linalg.norm(x.values), axis='columns')
        debug(pd.Timestamp('now'))

        # 从小到大排序，选取前k个最近的点
        diffMat = diffMat.sort_values(by='distant', ascending=True)
        diffMat = diffMat[0:k]
        debug(pd.Timestamp('now'))

        # 统计k个点的分类
        # diffMat = diffMat.groupby('result').size().to_frame(name='lable_count')
        diffMat = diffMat.groupby(diffMat.index).size().to_frame(name='lable_count')

        # 选取最多的分类
        inX_lable = diffMat['lable_count'].idxmax()

        return inX_lable

    @staticmethod
    def autoNorm(dataSet):
        """
        Desc：
            归一化特征值，消除特征之间量级不同导致的影响
        parameter：
            dataSet：数据集，pandas.DataFrame类型
        return：
            归一化后的数据集normDataSet，pandas.DataFrame类型

        原理：
            归一化公式：
                Y = (X-Xmin)/(Xmax-Xmin)
                其中的min和max分别是数据集中的最小特征值和最大特征值。
                该函数可以自动将数字特征值转化为0到1的区间。
        """

        # 计算每种属性的最大值、最小值、范围
        minVals = dataSet.min()
        debug(minVals)
        maxVals = dataSet.max()
        debug(maxVals)
        # 极差
        ranges = maxVals - minVals
        debug(ranges)
        m = len(dataSet)
        debug(m)

        normDataSet = pd.concat([minVals.to_frame().T] * m, ignore_index=True)
        debug(normDataSet)

        # 生成与最小值之差组成的矩阵
        normDataSet = dataSet - normDataSet.values
        debug(normDataSet)

        # 将最小值之差除以范围组成矩阵
        ranges_df = pd.concat([ranges.to_frame().T] * m, ignore_index=True)
        normDataSet = normDataSet / ranges_df.values
        debug(normDataSet)

        return normDataSet

    @staticmethod
    def datingTest():
        """
        收集数据：提供文本文件
        准备数据：使用 Python 解析文本文件
        分析数据：使用 Matplotlib 画二维散点图
        训练算法：此步骤不适用于 k-近邻算法
        测试算法：使用海伦提供的部分数据作为测试样本。
                测试样本和非测试样本的区别在于：
                    测试样本是已经完成分类的数据，如果预测分类与实际类别不同，则标记为一个错误。
        使用算法：产生简单的命令行程序，然后海伦可以输入一些特征数据以判断对方是否为自己喜欢的类型。
        """

        debug(pd.Timestamp('now'))
        dating_file = '../machinelearninginaction/Ch02/datingTestSet.txt'
        dating_data = pd.read_table(dating_file,
                                    header=None,
                                    names=['flight', 'game',
                                           'icecream', 'result'],
                                    index_col=3
                                    )
        debug(dating_data)
        debug(pd.Timestamp('now'))

        # cValue = ['r','y','g','b','r','y','g','b','r']
        # dating_data.plot(kind='scatter', x='flight', y='game', title='Dating', alpha=0.6)
        # dating_data.plot.scatter(x='flight', y='game', c='result', grid=True)

        # fig = plt.figure()
        # ax = fig.add_subplot(111)
        # ax.scatter(dating_data[:, 0], dating_data[:, 1])  # , 15.0*array(datingLabels), 15.0*array(datingLabels)

        largeDoses = dating_data.loc[dating_data.index == 'largeDoses']
        didntLike = dating_data.loc[dating_data.index == 'didntLike']
        smallDoses = dating_data.loc[dating_data.index == 'smallDoses']
        debug(pd.Timestamp('now'))

        ax = largeDoses.plot.scatter(
            x='flight', y='game', label='largeDoses', color='DarkBlue')
        ax = smallDoses.plot.scatter(
            x='flight', y='game', label='smallDoses', color='LightGreen', ax=ax)
        ax = didntLike.plot.scatter(
            x='flight', y='game', label='didntLike', color='Red', ax=ax)
        debug(pd.Timestamp('now'))

        # color_dict = {'largeDoses': 'b', 'smallDoses': 'g', 'didntLike': 'r'}  # col = ['b','r','g','c','y','m','k']
        # ax = dating_data.plot.scatter(x='flight', y='game', label='result', c=color_dict)

        plt.show()

        return dating_data


if __name__ == '__main__':
    # set_log(DEBUG)
    set_log(INFO)
    myKNN = kNN()
    dating_data = kNN.datingTest()
    dating_data = kNN.autoNorm(dating_data)
    hoRatio = 0.1
    numTestVecs = int(hoRatio * len(dating_data))
    errorCount = 0
    for i in range(numTestVecs):
        inX_lable = kNN.classify0(dating_data[i:i + 1], dating_data[0:], 10)
        info('Point %d, correct_label=%s, kNN_label=%s' % (i, inX_lable, dating_data.iloc[i, :].name))
        debug(pd.Timestamp('now'))
        if inX_lable != dating_data.iloc[i, :].name:
            errorCount += 1
    info('numTestVecs: %d' % numTestVecs)
    info('errorCount: %d' % errorCount)
    info('errorRate: %f' % float(errorCount / numTestVecs))
