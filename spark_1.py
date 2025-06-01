import sys
from pyspark import SparkContext

if(len(sys.argv) != 3):
  print("PLEASE PROVIDE INPUT AND OUTPUT FILES")
  sys.exit(0)

Sc = SparkContext
file = Sc.textFile(sys.argv[1])
temp = file.map(lambda line : (int(line[15:19]),int(line[87:92])))
max = temp.reduceByKey(lambda a,b : a if a<b else b)
max.saveAsTextFile(argv[2])
