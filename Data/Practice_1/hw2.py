from pyspark import SparkContext, SparkConf
from user_definition import *
# Do not add any additional libraries.
conf = SparkConf().setMaster("local[*]").setAppName(app_name)
#Initializing a SparkContext (SC)
sc = SparkContext(conf = conf)

file = sc.textFile(input_file)


numbers = file.flatMap(lambda x:x.split(","))
even = numbers.filter(lambda x:int(x)%2 == 0)
odd = numbers.filter(lambda x:int(x)%2 == 1)

Evens_sorted = even.map(lambda x:int(x)).sortBy(lambda x: x)
odds_sorted = odd.map(lambda x:int(x)).sortBy(lambda x: x)

print Evens_sorted.histogram(hist_size)
print odds_sorted.histogram(hist_size)

sc.stop()


