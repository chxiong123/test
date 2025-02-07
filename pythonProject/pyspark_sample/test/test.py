# 导包
from pyspark import SparkConf, SparkContext

# 为 PySpark 配置 Python 解释器
# 方式1
# import os
# os.environ['PYSPARK_PYTHON'] = "E:/Anaconda3/python.exe"
# 方式2
# import findspark
#
# findspark.init()

# 创建SparkConf类对象中
conf = SparkConf().setSparkHome("local[*]").setAppName("test_spqrk_app")

# 基SparkConf类对象创LSparkContext对象
sc = SparkContext(conf=conf)

arr = [1, 2, 3, 4, 66, 778]
rdd1 = sc.parallelize(arr)

print(sc.version)

print(rdd1.collect())


# 为每个元素执行的函数
def func(element):
    return element * 10


rdd2 = rdd1.map(func)
print("-----------------------")
print(rdd2.collect())
