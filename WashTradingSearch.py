
#RDD setup in python for GraphX Wash Trades processing in scala
import pyspark
import re
import time
from graphframes import *

def good_line_transactions(line):
    try:
        fields = line.split(',')
        if len(fields)!=7:
            return False
        float(fields[3])
        float(fields[5])
        if int(time.strftime("%m", time.gmtime(float(line.split(',')[6])))) == 8 and int(time.strftime("%Y", time.gmtime(float(line.split(',')[6])))) == 2018:
            return True
        else:
            return False
    except:
        return False

def good_line_blocks(line):
    try:
        fields = line.split(',')
        if len(fields)!=9:
            return False
        float(fields[6])
        float(fields[7])
        return True
    except:
        return False

def good_line_contracts(line):
    try:
        fields = line.split(',')
        if len(fields)!=5:
            return False
        float(fields[3])
        return True
    except:
        return False

def good_line_scams(line):
        try:
                fields = line.split(',')
                if len(fields)!=3:
                        return False
                return True
        except:
                return False


print('Running....')
sc = pyspark.SparkContext()
print('Spark Context Loaded')
sc.setLogLevel("ERROR")




print('Reading Transactions....')
transactions = sc.textFile('/data/ethereum/transactions')
good_transactions = transactions.filter(good_line_transactions)
addresses_origin = good_transactions.map(lambda x:(x.split(','[1]), 1))
addresses_dest = good_transactions.map(lambda x:(x.split(','[2]), 1))
red_addresses_origin = addresses_origin.reduceByKey(lambda x,y: x+y)
red_addresses_dest = addresses_origin.reduceByKey(lambda x,y: x+y)
vertices=red_addresses_origin.fullOuterJoin(red_addresses_dest)

#addressID, tot_transactions
Vdf = vertices.toDF(['ID', 'Total Transactions'])

edges = good_transactions.map(lambda x:( x.split(',')[1],   x.split(',')[2], float(x.split(',')[3]))) 
#vertex1,vertex2,value
Edf = edges.toDF(['Origin', 'Destination','Value'])


print('Ordering.....')
scamsByTypeReduced.saveAsTextFile('scamsbyyear2018')