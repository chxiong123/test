# spark 程序编写：提交代码到集群运行
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    print("提交代码到集群运行")

    # 1.创建sparkContext对象，连接Spark集群，有多少台集群写多少
    conf = SparkConf().setSparkHome("spark://node1:7077,node2:7077").setAppName("wordcount")
    sc = SparkContext(conf=conf)

    # 2.读取hdfs文件，需要确保hdfs启动服务
    # 读取hdfs文件协议：hdfs://localhost:9000/  该IP端口是根据本地在按照hadoop的时候设定的，可以改变
    rdd1 = sc.textFile("hdfs://localhost:9000/user/Dell/input/word.txt")  # 可执行

    # 3.将读取到的每一行数据，进行切割操作，然后得到一个大列表，放置每个单词
    rdd2 = rdd1.flatMap(lambda line: line.split(" "))

    rdd3 = rdd2.map(lambda word: (word, 1))

    rdd4 = rdd3.reduceByKey(lambda agg, curr: agg + curr)

    print(rdd4.collect())

    rdd2.saveAsTextFile("hdfs://localhost:9000/user/Dell/input/input_del2")  # output_test必须是没有创建过的

    # 关闭对象，结束进场
    sc.stop()
