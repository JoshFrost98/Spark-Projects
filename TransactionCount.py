import pyspark
import re
import time

sc = pyspark.SparkContext()
sc.setLogLevel("ERROR")


def good_line_blocks(line):
    try:
        fields = line.split(',')
        if len(fields)!=9:
            return False
        float(fields[7])
        return True
    except:
        return False



blocks = sc.textFile("/input/blocks")
good_blocks = blocks.filter(good_line_blocks)
month = good_blocks.map ( lambda t: ( time.strftime("%M", time.gmtime(float(t.split(',')[7]))), 1 ))
results = month.reduceByKey(lambda a,b: a+b)
results.saveAsTextFile('outq4q3')