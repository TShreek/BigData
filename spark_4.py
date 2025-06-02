import sys
from pyspark import SparkContext

sc = SparkContext()
file = sc.textFile(sys.argv[1])
temp = file.map(lambda line : (line.split(" ")[7],1))
data = temp.countByKey()
dd=sc.parallelize(data.items())
dd.saveAsTextFile(sys.argv[2])
