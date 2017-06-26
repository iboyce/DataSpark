#coding:utf-8

import pymssql
import time


start = time.clock()
#get user stk data
conn = pymssql.connect(host='10.237.2.40', user='fangke', password='fangke', database='ydzq')
cursor = conn.cursor()
strQuery ="select  KEY_VALUE,STOCK_CODE from ydzq..tbl_user_stock"
#strQuery ="select  KEY_VALUE,STOCK_CODE from ydzq..tbl_user_stock"
cursor.execute(strQuery)





row = cursor.fetchone()
usrStk = dict()
stkUsr = dict()


#预处理一：取数，建立两张表
while row:
    #print("userId=%s, stkCode=%s" % (row[0], row[1]))

    #if  row[1] in ('399001','000001') :     #也可
    if (row[1]=='399001') or (row[1]=='000001'):          #筛除特殊情况
        row = cursor.fetchone()
        continue
        
    usrStk.setdefault(row[0],set()).add(row[1])                            #建立用户，自选股对应表
    stkUsr.setdefault(row[1],set()).add(row[0])                            #建立反向表
    
    row = cursor.fetchone()
    
#预处理二：在反向表中筛选有关系客户
usrTusr = dict()

for key in stkUsr:
    if len(stkUsr[key]) <= 1:
           continue

    for usr in stkUsr[key]:
           #print (usr, stkUsr[key])
        otherUsr = stkUsr[key] - set([usr])                  #注意set(usr),set([usr])区别
        
           #print (usr,set([usr]),otherUsr)
        usrTusr.setdefault(usr,set()).update(otherUsr)
           #print (usrTusr)

###计算有关联客户的相似度
##simUsr = dict()
##
##for usrX in usrTusr:  
##
##    max = 0
###    maxUsrY = ""
##    for usrY in usrTusr[usrX]:                                                                 #TOP K=1
##        dist = (len(usrStk[usrX]&usrStk[usrY])*1.0)/len(usrStk[usrX]|usrStk[usrY])
##	print(dist)
##        if ( dist > max ):
##            max = dist
##            maxUsrY = usrY
##    
##    print(maxUsrY)
##    simUsr.setdefault(usrX,list()).append(max)
##    simUsr.setdefault(usrX,list()).append(maxUsrY)
##



#Debug::print 10 userStk
##print("number of stk and usr--------------------------------------")
print(len(usrStk),len(stkUsr),len(usrTusr))
           
##k=100 
##i=0
##print("10 usrStk--------------------------------------")
##for key in usrStk:    
##    print(i,key,usrStk[key])
##    i=i+1
##    if i==k:
##        break
##print("10 stkUsr--------------------------------------")
##i=0
##for key in stkUsr:    
##    print(i,key,stkUsr[key])
##    i=i+1
##    if i==k:
##        break
##
##print("10 usrTusr--------------------------------------")
##i=0
##for key in usrTusr:    
##    print(i,key,usrTusr[key])
##    i=i+1
##    if i==k:
##        break
##
##
##print("10 simUsr--------------------------------------")
##i=0
##for usr in simUsr:
##    sim = simUsr[usr][0]
##    simU = simUsr[usr][1]
##
##    print(simUsr)
##    print(usr,sim,simU)
##    print (usrStk.get(usr))
##    print("用户：",usr,"最大相似度：",sim,"对应股票推荐：",usrStk[simU]-usrStk[usr])
##    
##    i=i+1
##    if i==k:
##        break





conn.close()

end = time.clock()
print("ElapsedTime:%f s"%(end-start))

