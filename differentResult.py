#coding:utf-8

import pymssql
import time
import sys

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

class usrTusr_AccumulatorParam(AccumulatorParam):
    def zero(self, initialValue):
        return dict()

    def addInPlace(self, v1, v2):
	
	if type(v2)==dict:
	  return v2
	
	for usr in v2[1]:
	  otherSet=v2[1]-set([usr])
	  v1.setdefault(usr,set()).update(otherSet)


	return v1

start = time.clock()
#get user stk data
conn = pymssql.connect(host='10.237.2.40', user='fangke', password='fangke', database='ydzq')
cursor = conn.cursor()
strQuery ="select top 100000 KEY_VALUE,STOCK_CODE from ydzq..tbl_user_stock"
#strQuery ="select  KEY_VALUE,STOCK_CODE from ydzq..tbl_user_stock"
cursor.execute(strQuery)





row = cursor.fetchone()
usrStk = dict()
stkUsr = dict()


#预处理一：取数，建立两张表
while row:
#  print("userId=%s, stkCode=%s" % (row[0], row[1]))

#if  row[1] in ('399001','000001') :     #也可
  if (row[1]=='399001') or (row[1]=='000001'):          #筛除特殊情况
    row = cursor.fetchone()
    continue


  usrStk.setdefault(row[0],set()).add(row[1])                            #建立用户，自选股对应表
  stkUsr.setdefault(row[1],set()).add(row[0])                            #建立反向表

  row = cursor.fetchone()



#conf = SparkConf().setAppName("Hello").setMaster("local[1]")
conf = SparkConf().setAppName("Hello").setMaster("spark://10.237.2.130:7077").set("spark.executor.memory","9g").set("spark.default.parallelism","96").set("spark.driver.memory","9g").set("spark.driver.maxResultSize","8g")

sc = SparkContext(conf=conf)


usrTusr_Accum = sc.accumulator({}, usrTusr_AccumulatorParam())


rdd_stkUsr=sc.parallelize(dict_to_list(stkUsr))
rdd_stkUsr.foreach(lambda x:add(x,usrTusr_Accum))
#print sc.parallelize(dict_to_list(stkUsr),250).collect()

usrTusr = usrTusr_Accum.value



conn.close()

end = time.clock()
print("ElapsedTime:%f s"%(end-start))

print("len of data",len(usrStk),len(stkUsr),sys.getsizeof(usrStk),sys.getsizeof(stkUsr))

#debug info
#k=10 
#i=0
#print("10 usrStk--------------------------------------")
#for key in usrStk:    
#    print(i,key,usrStk[key])
#    i=i+1
#    if i==k:
#        break
#print("10 stkUsr--------------------------------------")
#i=0
#for key in stkUsr:    
#    print(i,key,stkUsr[key])
#    i=i+1
#    if i==k:
#        break
#
#print("10 usrTusr--------------------------------------")
#i=0
#for key in usrTusr:    
#    print(i,key,usrTusr[key])
#    i=i+1
#    if i==k:
#        break

debug_a=usrTusr.get('100199')
if debug_a!=None:
  print(len(usrTusr),len(debug_a),debug_a)
else:
  print(len(usrTusr))

