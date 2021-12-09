import pyspark
import re
import time

sc = pyspark.SparkContext()
sc.setLogLevel("ERROR")


def good_line_transactions(line):
    try:
        fields = line.split(',')
        if len(fields)!=7:
            return False
        float(fields[3])
        float(fields[5])
        return True
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

def filter_transactions_by_contract(line):
    try:
        return (line[0], line[1][0][0], line[1][0][1], line[1][0][2], line[1][0][3], line[1][1])
    except:
        pass

blocks = sc.textFile('/data/ethereum/blocks')
good_blocks = blocks.filter(good_line_blocks)
miners = good_blocks.map(lambda x: (x.split(',')[2], (float(x.split(',')[4]), 1)))
minertot = miners.reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1]))
newminertot = miners.map(lambda x: (x[1][0], x[1][1], x[1][0]/x[1][1]))
top10miners = minertot.takeOrdered(10, key=lambda x: -x[1])
print(top10miners)
print('')
print('Top Miners')
for miner in top10miners:
    print('Miner: {}, Total Block Size Mined {}, Total Blocks Mined {}, Average Block Size{}'.format(miner[0], miner[1], miner[2], miner[3]))

