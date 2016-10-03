import re
from pyspark import SparkConf, SparkContext

def parseLine(line):
    fields = line.split(',')
    customerId = int(fields[0])
    value = float(fields[2])
    return (customerId, value)


conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

lines = sc.textFile('customer-orders.csv')
ordersValue = lines.map(parseLine).reduceByKey(lambda x, y: x + y)
sortedResults = ordersValue.map(lambda (x, y): (y, x)).sortByKey()

for order in sortedResults.collect():
    print order
