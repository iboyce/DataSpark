from pyspark.ml.feature import PCA
from pyspark.ml.linalg import Vectors
import time
import sys
from pyspark import SparkContext, SparkConf, AccumulatorParam
from pyspark.sql import Row,SparkSession
import numpy as np

from pyspark.ml.feature import StandardScaler

reload(sys)
sys.setdefaultencoding('utf8')


start = time.clock()

#connect to spark cluster
conf = SparkConf().set("spark.default.parallelism","10").set("spark.driver.memory","4g").set("spark.executor.memory","4g")
spark = SparkSession \
    .builder \
    .master("spark://10.237.2.130:7077")  \
    .appName("PCA") \
    .config("spark.default.parallelism","80") \
    .config("spark.driver.memory","8g") \
    .config("spark.executor.memory","8g") \
    .config("spark.speculation","true") \
    .config("spark.local.dir","/opt/tmp") \
    .getOrCreate()

sc = spark.sparkContext

#data = sc.textFile("file:///opt/workspace/tgtag0528.csv")
df = spark.read.csv("hdfs://jf-dis0:9000/data/alltag0530.csv",header=True)
df = df.dropna()
p=1.0
df = df.sample(False,p,42)
gf=df.select("*").rdd.map(lambda x:(x[0],Vectors.dense(np.array([x[1],x[2],x[3],x[4],x[5],x[6],x[7],x[8],x[9],x[10],x[11],x[12],x[13],x[14],x[15],x[16]]))))

ef = spark.createDataFrame(gf,["id","feature"])

scaler = StandardScaler(inputCol="feature", outputCol="scaledFeatures",
                        withStd=True, withMean=False)

scalerModel = scaler.fit(ef)


scaledData = scalerModel.transform(ef)

pca = PCA(k=15, inputCol="scaledFeatures", outputCol="pcaFeatures")
model = pca.fit(scaledData)

result = model.transform(scaledData).select("id","pcaFeatures")

rr=result.rdd.map(lambda x: (x[0],float(x[1][0]),float(x[1][1]),float(x[1][2])))
#write to dis file
rrdd =  spark.createDataFrame(rr)
#rrdd.write.csv("/opt/pca3_06211844")
#write to 'one' local file 
import csv

csvfile=file("/opt/workspace/pca1044.csv",'wb')
writer = csv.writer(csvfile)
writer.writerows(rr.collect())
csvfile.close()


print model.explainedVariance

print rrdd.show()


end0 = time.clock()
print("GetDataElapsedTime:%f s"%(end0-start))
