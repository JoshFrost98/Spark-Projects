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



transactions = sc.textFile("/data/ethereum/transactions")
good_transactions = transactions.filter(good_line_transactions)
to_address = good_transactions.map ( lambda t: ( t.split(',')[2], float(t.split(',')[3]) ))
results_to_address = to_address.reduceByKey(lambda a,b: a+b)
top10addresses = results_to_address.takeOrdered(10, key=lambda x: -x[1])
print('')
print('Top Addresses')
for address in top10addresses:
    print('Adress: {}, Total Wei Sent{}'.format(address[0], address[1]))
