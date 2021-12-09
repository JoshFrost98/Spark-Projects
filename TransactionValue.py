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




transactions = sc.textFile("/data/ethereum/transactions")
good_transactions = transactions.filter(good_line_transactions)
month = good_transactions.map ( lambda t: (str(time.strftime("%Y", time.gmtime(float(t.split(',')[6])))) + str(time.strftime("%m", time.gmtime(float(t.split(',')[6])))), (float(t.split(',')[3], 1) ))
tot_monthly_transactions = month.reduceByKey(lambda a,b: (float(a[0])+float(b[0]), a[1]+b[1]))
ave_monthly_transactions = tot_monthly_transactions.map(lambda a: (a[0], a[1][0]/a[1][1]))
ave_monthly_transactions.saveAsTextFile('outCW1q2')
tot_monthly_transactions.saveAsTextFile('outCW1q2tot')