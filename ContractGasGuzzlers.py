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

        float(fields[0])
        float(fields[3])
        float(fields[4])
        float(fields[5])
        float(fields[6])

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
def filter_year(line):
    if line[2] == '2019':
        return True
    else:
        return False
def other(line):
    if line[1]!='true' and line[2]!='true':
        return True
    else:
        return False

def erc20(line):
    if line[1]=='true':
        return True
    else:
        return False

def erc721(line):
    if line[2]=='true':
        return True
    else:
        return False



transactions = sc.textFile("/data/ethereum/transactions")
rawcontracts = sc.textFile('/data/ethereum/contracts')
good_transactions = transactions.filter(good_line_transactions)
good_contracts = rawcontracts.filter(good_line_contracts)
contracts = good_contracts.map(lambda x: (x.split(',')[0], x.split(',')[1] + x.split(',')[2]))


year = good_transactions.map ( lambda t: ( t.split(',')[2], float(t.split(',')[4]), str(time.strftime("%Y", time.gmtime(float(t.split(',')[6])))) ))
filtered = year.filter(filter_year)#address, gas, date
mapped = filtered.map(lambda x: (x[0], x[1]))#address, gas
with_contracts = contracts.join(mapped)#address, (gas, contract)



mapped_with_contracts = with_contracts.map(lambda x: (x[1][0], (x[1][1], 1)))#contract, (gas, 1)
tot_monthly_gas = mapped_with_contracts.reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1]))#truefalse, (totalgas, count)
ave_monthly_gas = tot_monthly_gas.map(lambda a: (a[0], a[1][0]/a[1][1]))#contract, avegas
Result = tot_monthly_gas.takeOrdered(10, key=lambda x: -x[1][0])
Result = tot_monthly_gas.take(10)
print('Ordering.....')
for record in Result:
    print("2019: Contract Type:{}, Total Gas Used:{}".format(record[0],record[1]))
ResultA = ave_monthly_gas.takeOrdered(10, key=lambda x: -x[1])
ResultA = ave_monthly_gas.take(10)
for record in ResultA:
    print("2019: Contract Type:{}, Average Gas Used:{}".format(record[0],record[1]))