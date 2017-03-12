from __future__ import print_function
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from datetime import datetime

# initialize streaming context
conf = SparkConf().setMaster("local[2]").setAppName("TradeVolume")
sc = SparkContext(conf = conf)
#sc = SparkContext(appName="SparkStreamingCountBuys") 
ssc = StreamingContext(sc, 4)

# create DStream using filestream
HDFS_DATA="/user/cloudera/CSCIE63/HW5/p1"
filestream = ssc.textFileStream("hdfs://"+HDFS_DATA+"/input")

# callback to parse each input line
def parseOrder(line):
  s = line.split(",")
  try:
      if s[6] != "B" and s[6] != "S":
        raise Exception('Wrong format')

      return [{"time": datetime.strptime(s[0], "%Y-%m-%d %H:%M:%S"), "orderId": long(s[1]), "clientId": long(s[2]), "symbol": s[3], "amount": int(s[4]), "price": float(s[5]), "buy": s[6] == "B"}]

  except Exception as err:
      print("Wrong line format (%s): " % line)
      return []

orders = filestream.flatMap(parseOrder)

# count total traded volume for each symbol
from operator import add
volPerSymbol= orders.map(lambda o: (o['symbol'], o['amount'])).reduceByKey(add);

# transform, sort and get the top-5 symbols
topVolume = volPerSymbol.transform(lambda rdd: rdd.sortBy(lambda x: x[1], False).zipWithIndex().filter(lambda x: x[1] < 1))
#topVolume.pprint()

topVolume.repartition(1).saveAsTextFiles("hdfs://"+HDFS_DATA+"/output/output", "txt")

ssc.start()
ssc.awaitTermination()
# ssc.stop(False)

