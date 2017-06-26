#coding:utf-8

import pymssql
import time
import sys

from pyspark import SparkContext, SparkConf, AccumulatorParam

def add(x,Accum):                   #lambda表达式，此处操作非原子
  Accum += x

def dict_to_list(d):                #dict转成list,list可直接转成dict,前提是每个元素有成对值，比如[[u'300007', set([u'100367'])], [u'002488', set([u'100306'])]]
    a = []
    for key, value in d.iteritems():
        if (type(value) is dict):
            value = dict_to_list(value)
        a.append([key, value])
    return a

class usrTusr_AccumulatorParam(AccumulatorParam):               #自定义usrTusr累加器
    def zero(self, initialValue):
        return {}

    def addInPlace(self, v1, v2):
        if type(v2)==list:
          v1[v2[0]]=v2[1]
        else:
          for key in v2:
            if len(v2[key]) <= 1:
               continue
            for usr in v2[key]:
              otherUsr = v2[key] - set([usr])
              v1.setdefault(usr,set()).update(otherUsr)       
        return v1

start = time.clock()

conn = pymssql.connect(host='10.237.2.40', user='fangke', password='fangke', database='ydzq')           #连接数据源
cursor = conn.cursor()
strQuery ="select  KEY_VALUE,STOCK_CODE from ydzq..tbl_user_stock"
#strQuery ="select  KEY_VALUE,STOCK_CODE from ydzq..tbl_user_stock"
cursor.execute(strQuery)


usrStk = dict()
stkUsr = dict()

#预处理一：取数，建立两张表
row = cursor.fetchone()
while row:
  #print("userId=%s, stkCode=%s" % (row[0], row[1]))

#if  row[1] in ('399001','000001') :     #也可
  if (row[1]=='399001') or (row[1]=='000001'):          #筛除特殊情况
    row = cursor.fetchone()
    continue

  usrStk.setdefault(row[0],set()).add(row[1])                            #建立用户，自选股对应表
  stkUsr.setdefault(row[1],set()).add(row[0])                            #建立反向表

  row = cursor.fetchone()

#print(stkUsr)
print('stkUsr','stkUsr',len(stkUsr),len(usrStk),sys.getsizeof(stkUsr),sys.getsizeof(usrStk))

conf = SparkConf().setAppName("Hello").setMaster("spark://10.237.2.130:7077").set("spark.default.parallelism","80").set("spark.driver.memory","1g")      #连接计算集群，设置参数
sc = SparkContext(conf=conf)


usrTusr_Accum = sc.accumulator(dict(), usrTusr_AccumulatorParam())                                      #定义累加器

sc.parallelize(dict_to_list(stkUsr)).foreach(lambda x:add(x,usrTusr_Accum))

usrTusr=usrTusr_Accum.value                                                   #driver取累加器值

print(len(usrStk),len(stkUsr),len(usrTusr))                                     

conn.close()

end = time.clock()

print("ElapsedTime:%f s"%(end-start))
