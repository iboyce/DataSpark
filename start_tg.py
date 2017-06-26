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

#data = sc.textFile("file:///opt/workspace/tgtag0528.csv")
df = spark.read.csv("file:///opt/workspace/tgtag0528.csv")
df3=df.select("_c1","_c4","_c8").dropna().rdd.map(lambda x:array(list(x)))
clusters = KMeans.train(df3, 3, maxIterations=10, initializationMode="random")


print clusters.clusterCenters
# Evaluate clustering by computing Within Set Sum of Squared Errors
#def error(point):
#   center = clusters.centers[clusters.predict(point)]
#    return sqrt(sum([x**2 for x in (point - center)]))

#WSSSE = df3.map(lambda point: error(point)).reduce(lambda x, y: x + y)
print("Within Set Sum of Squared Error = " + str(clusters.computeCost(df3)))


k=100
for i in range(2,k):
	clusters = KMeans.train(df3, i, maxIterations=100)
	print("%d class:Within Set Sum of Squared Error = "%(i) + str(clusters.computeCost(df3)))
# Save and load model
#clusters.save(sc, "KMeansModel")
#sameModel = KMeansModel.load(sc, "KMeansModel")




end0 = time.clock()
print("GetDataElapsedTime:%f s"%(end0-start))

