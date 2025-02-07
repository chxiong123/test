# spark 程序编写：实现WordCount案例（local模式）
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    print("读取本地文件")

    # 1.创建sparkContext对象
    conf = SparkConf().setSparkHome("local[*]").setAppName("wordcount")
    sc = SparkContext(conf=conf)

    # 2.读取本地文件：file:///
    rdd1 = sc.textFile("file:///D:\hxc\python_workspace\pythonProject\pyspark_sample\data\word.txt")

    # 2.读取hdfs文件，需要确保hdfs启动服务
    # 读取hdfs文件协议：hdfs://localhost:9000/  该IP端口是根据本地在按照hadoop的时候设定的，可以改变
    # rdd1 = sc.textFile("hdfs://localhost:9000/user/Dell/input/word.txt") # 可执行

    # 3. 将读取到的每一行数据，进行切割操作，然后得到一个大列表，放置每个单词
    # rdd2 = rdd1.map(lambda line: line.split(" ")) # [['hadoop', 'java', 'hive', 'sqoop'], ['hadoop', 'hadoop', 'java'], ['spark', 'spark'], ['pig', 'hbase', 'hbase']]
    rdd2 = rdd1.flatMap(lambda line: line.split(" "))  # ['hadoop', 'java', 'hive', 'sqoop', 'hadoop', 'hadoop', 'java', 'spark', 'spark', 'pig', 'hbase', 'hbase']

    rdd3 = rdd2.map(lambda word:(word, 1))

    rdd4 = rdd3.reduceByKey(lambda agg, curr: agg + curr)

    print(rdd4.collect())

    # rdd2.saveAsTextFile("hdfs://localhost:9000/user/Dell/input/input_del")  # output_test必须是没有创建过的
    rdd2.saveAsTextFile("file:///D:\hxc\python_workspace\pythonProject\pyspark_sample\data\output_test")  # output_test必须是没有创建过的

    sc.stop()











