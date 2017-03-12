from __future__ import print_function
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from datetime import datetime

# initialize streaming context
conf = SparkConf().setMaster("local[2]").setAppName("Top5Volumes")
sc = SparkContext(conf = conf)
ssc = StreamingContext(sc, 9)
ssc.checkpoint("/user/cloudera")

# create DStream using filestream
HDFS_DATA="/user/cloudera/CSCIE63/HW5/p2"
filestream = ssc.textFileStream("hdfs://"+HDFS_DATA+"/input")

# callback to parse each input line
def parseOrder(line):
  s = line.split(",")
  try:
      if s[6] != "B" and s[6] != "S":
        raise Exception('Wrong format')

      #print("Received input line (%s): " % line)
      return [{"time": datetime.strptime(s[0], "%Y-%m-%d %H:%M:%S"), "orderId": long(s[1]), "clientId": long(s[2]), "symbol": s[3], "amount": int(s[4]), "price": float(s[5]), "buy": s[6] == "B"}]

  except Exception as err:
      print("Wrong line format (%s): " % line)
      return []

# callback for updateStateByKey to remember running top 5 stocks
def updateVolume(currentVolume, volumeState):
    # init for the first batch
    if volumeState is None:
        volumeState = 0

    return  sum(currentVolume, volumeState)

# build a flat map for each stock order
orders = filestream.flatMap(parseOrder)

# find total traded volume for each symbol
from operator import add
symVolumesCurrent= orders.map(lambda o: (o['symbol'], o['amount'])).reduceByKey(add);

# update traded volumes with the previous run
symbolVolumes = symVolumesCurrent.updateStateByKey(updateVolume)
symbolVolumes.pprint()

# current batch: transform, sort and get the top-5 traded symbols
top5VolumesCurrent = symVolumesCurrent.transform(lambda rdd: rdd.sortBy(lambda x: x[1], False).zipWithIndex().filter(lambda x: x[1] < 5))
top5VolumesCurrent.repartition(1).saveAsTextFiles("hdfs://"+HDFS_DATA+"/output/output", "txt")

# running counter: transform, sort and print the top-5 traded symbols
top5Volumes = symbolVolumes.transform(lambda rdd: rdd.sortBy(lambda x: x[1], False).zipWithIndex().filter(lambda x: x[1] < 5))
top5Volumes.repartition(1).saveAsTextFiles("hdfs://"+HDFS_DATA+"/running/output", "txt")

ssc.start()
ssc.awaitTermination()
# ssc.stop(False)

