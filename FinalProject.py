!apt-get install openjdk-8-jdk-headless -qq > /dev/null
!wget -q http://archive.apache.org/dist/spark/spark-2.3.2/spark-2.3.2-bin-hadoop2.7.tgz
!tar xf spark-2.3.2-bin-hadoop2.7.tgz
!pip install -q pyspark
!pip install gdelt

import pyspark
import gdelt
gd2 = gdelt.gdelt(version=2)
from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local[*]").set("spark.executor.memory", "1g")
sc = SparkContext(conf = conf)

#also include this short helper function for use later in this lab
def dbg(x):
  """ A helper function to print debugging information on RDDs """
  if isinstance(x, pyspark.RDD):
    print([(t[0], list(t[1]) if 
            isinstance(t[1], pyspark.resultiterable.ResultIterable) else t[1])
           if isinstance(t, tuple) else t
           for t in x.take(100)])
  else:
    print(x)

results = gd2.Search(['2016 11 01'], table='events', coverage=True)
goldstein_list = sc.parallelize(results['GoldsteinScale'].tolist())
def map_Goldstein(x):
  if x < -5:
    return -2
  elif x < 0:
    return -1
  elif x > 5:
    return 2
  elif x >= 0:
    return 1
  
goldstein_mapped = goldstein_list.map(map_Goldstein)
dbg(goldstein_mapped.take(60))