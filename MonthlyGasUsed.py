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

def transaction_mapper(line):
    date = str(time.strftime("%Y", time.gmtime(float(line.split(',')[6])))) + str(time.strftime("%m", time.gmtime(float(line.split(',')[6]))))
    value = float(line.split(',')[4])
    yield date, (value, 1)


# transactions = sc.textFile("/data/ethereum/transactions")
# good_transactions = transactions.filter(good_line_transactions)
# month = good_transactions.map (transaction_mapper)
# tot_monthly_gas = month.reduceByKey(lambda a,b: (float(a[0])+float(b[0]), a[1]+b[1]))
# ave_monthly_gas = tot_monthly_gas.map(lambda a: (a[0], a[1][0]/a[1][1]))
# ave_monthly_gas.saveAsTextFile('AverageGas')
# tot_monthly_gas.saveAsTextFile('TotalGas')




transactions = sc.textFile("/data/ethereum/transactions")
good_transactions = transactions.filter(good_line_transactions)
month = good_transactions.map ( lambda t: (str(time.strftime("%Y", time.gmtime(float(t.split(',')[6])))) + str(time.strftime("%m", time.gmtime(float(t.split(',')[6])))), (float(t.split(',')[4]), 1) ))
tot_monthly_transactions = month.reduceByKey(lambda a,b: (float(a[0])+float(b[0]), a[1]+b[1]))
ave_monthly_transactions = tot_monthly_transactions.map(lambda a: (a[0], a[1][0]/a[1][1]))
ave_monthly_transactions.saveAsTextFile('AveGas')
tot_monthly_transactions.saveAsTextFile('TotGas')