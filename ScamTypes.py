
import pyspark
import re
import time

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

def good_line_scams(line):
        try:
                fields = line.split(',')
                if len(fields)!=3:
                        return False
                return True
        except:
                return False

def transaction_mapper(line):
    year = int(time.strftime("%Y", time.gmtime(float(line.split(',')[6]))))
    value = float(line.split(',')[3])
    address = line.split(',')[2]
    return address, value, year

def filter_year(line):
    if line[2] == 2018:
        return True
    else:
        return False

print('Running....')
sc = pyspark.SparkContext()
print('Spark Context Loaded')
sc.setLogLevel("ERROR")



print('Reading Scams.....')
scams = sc.textFile('scams.csv')
scams_filtered = scams.filter(good_line_scams)


print('mapping Addresses to scam type....')
scam_address_by_id = scams.map(lambda x:( x.split(', ')[1], x.split(', ')[0]  ))

print('Reading Transactions....')
transactions = sc.textFile('/data/ethereum/transactions')
good_transactions = transactions.filter(good_line_transactions)
values = good_transactions.map(lambda x:( x.split(',')[2] , float(x.split(',')[3]) ))

address_total_recieved = values.reduceByKey(lambda a,b: a+b)

print('Joining Transaction dataset with scam type')
scam_addresses_recieved = scam_address_by_id.join(address_total_recieved)
scam_val_by_type = scam_addresses_recieved.map(lambda x:(   x[1][0], x[1][1]  ))
scamsByTypeReduced = scam_val_by_type.reduceByKey(lambda a,b: a+b )

print('Ordering.....')
TopTenScams = scamsByTypeReduced.takeOrdered(10, key=lambda x: -x[1])
for record in TopTenScams:
    print(": Scam ID:{}, Total Wei Recieved:{}".format(record[0],record[1]))

                                                                                                                      