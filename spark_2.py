import sys
from pyspark import SparkContext

if(len(sys.argv != 3)):
  print("PLEASE PROVIDE INPUT AND OUTPUT FILES")
  sys.exit(0)

sc = SparkContext
file = sc.textFile(sys.argv[1])
temp = file.map(lambda x: x.split(",")[16], 1))
data = temp.countByKey()
dd = sc.parallelize(data.items())
dd.saveAsTextFile(sys.argv[2])
