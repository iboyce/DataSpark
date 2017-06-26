from pyspark.mllib.fpm import FPGrowth

import time
from numpy import array
from math import sqrt
import sys

reload(sys)
sys.setdefaultencoding('utf8')

from pyspark import SparkContext, SparkConf, AccumulatorParam
from pyspark.sql import Row,SparkSession
from pyspark.mllib.clustering import KMeans, KMeansModel

start = time.clock()

#connect to spark cluster
conf = SparkConf().set("spark.default.parallelism","10").set("spark.driver.memory","4g").set("spark.executor.memory","4g")
spark = SparkSession \
    .builder \
    .master("local") \
    .appName("Python Spark SQL basic example") \
    .config("spark.default.parallelism","80") \
    .config("spark.driver.memory","8g") \
    .config("spark.executor.memory","8g") \
    .config("spark.speculation","true") \
    .config("spark.local.dir","/opt/tmp") \
    .getOrCreate()

sc = spark.sparkContext




#data = sc.textFile("/opt/spark-2.0.1-bin-hadoop2.6/data/mllib/sample_fpgrowth.txt")
data = sc.textFile("allbasket.txt")


transactions = data.map(lambda line: line.strip().split(' '))
model = FPGrowth.train(transactions, minSupport=0.001, numPartitions=10)
result = model.freqItemsets().collect()
for fi in result:
    print(fi)



end0 = time.clock()
print("GetDataElapsedTime:%f s"%(end0-start))
