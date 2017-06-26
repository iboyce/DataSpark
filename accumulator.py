#coding:utf-8

import pymssql
import time

from pyspark import SparkContext, SparkConf, AccumulatorParam

def add(x,Accum):
  Accum += x


def dict_to_list(d):
    a = []
    for key, value in d.iteritems():
        if (type(value) is dict):
            value = dict_to_list(value)
        a.append([key, value])
    return a



class VectorAccumulatorParam(AccumulatorParam):
    def zero(self, initialValue):
        return []

    def addInPlace(self, v1, v2):
        v1 += v2
        return v1

class DictAccumulatorParam(AccumulatorParam):
    def zero(self, initialValue):
        return dict()

    def addInPlace(self, v1, v2):
        print('v1',v1,type(v1))
	print('v2',v2,type(v2))
	
	if type(v2)==dict:
	  return v2

	if len(v2)>1:
	 
          v1.setdefault(v2[0],set())
	  v1[v2[0]] |= v2[1]	
	#print('v1',v1)
	return v1

start = time.clock()
#get user stk data
conn = pymssql.connect(host='10.237.2.40', user='fangke', password='fangke', database='ydzq')
cursor = conn.cursor()
strQuery ="select top 70 KEY_VALUE,STOCK_CODE from ydzq..tbl_user_stock"
#strQuery ="select  KEY_VALUE,STOCK_CODE from ydzq..tbl_user_stock"
cursor.execute(strQuery)





row = cursor.fetchone()
usrStk = dict()
stkUsr = dict()


#预处理一：取数，建立两张表
while row:
  print("userId=%s, stkCode=%s" % (row[0], row[1]))

#if  row[1] in ('399001','000001') :     #也可
  if (row[1]=='399001') or (row[1]=='000001'):          #筛除特殊情况
    row = cursor.fetchone()
    continue


  usrStk.setdefault(row[0],set()).add(row[1])                            #建立用户，自选股对应表
  stkUsr.setdefault(row[1],set()).add(row[0])                            #建立反向表

  row = cursor.fetchone()


print(stkUsr)

conf = SparkConf().setAppName("Hello").setMaster("local[1]")
#conf = SparkConf().setAppName("Hello").setMaster("spark://10.237.2.130:7077")
sc = SparkContext(conf=conf)


#vecAccum = sc.accumulator([], VectorAccumulatorParam())

#sc.parallelize(dict_to_list(stkUsr)).foreach(lambda x:add(x,vecAccum))

dictAccum = sc.accumulator([], DictAccumulatorParam())

print(dict_to_list(stkUsr))

sc.parallelize(dict_to_list(stkUsr)).foreach(lambda x:add(x,dictAccum))

print(dictAccum)

conn.close()

end = time.clock()
print("ElapsedTime:%f s"%(end-start))
