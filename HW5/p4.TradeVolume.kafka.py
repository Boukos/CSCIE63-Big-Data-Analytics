#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
 Counts words in UTF8 encoded, '\n' delimited text directly received from Kafka in every 2 seconds.
 Usage: direct_kafka_wordcount.py <broker_list> <topic>

 To run this on your local machine, you need to setup Kafka and create a producer first, see
 http://kafka.apache.org/documentation.html#quickstart

 and then run the example
    `$ bin/spark-submit --jars \
      external/kafka-assembly/target/scala-*/spark-streaming-kafka-assembly-*.jar \
      examples/src/main/python/streaming/direct_kafka_wordcount.py \
      localhost:9092 test`
"""
from __future__ import print_function

import sys

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from datetime import datetime
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

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: direct_kafka_wordcount.py <broker_list> <topic>", file=sys.stderr)
        exit(-1)

    # setMaster to "local"
    conf = SparkConf().setMaster("local[2]").setAppName("KafaTopVolume")
    sc = SparkContext(conf = conf)
    ssc = StreamingContext(sc, 10)

    brokers, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})

    # build a flat map for each stock order
    lines = kvs.map(lambda x: x[1])
    orders = lines.flatMap(parseOrder)

    # find total traded volume for each symbol
    from operator import add
    symVolumesCurrent= orders.map(lambda o: (o['symbol'], o['amount'])).reduceByKey(add);

    # transform, sort and get the top-5 symbols
    topVolume = symVolumesCurrent.transform(lambda rdd: rdd.sortBy(lambda x: x[1], False).zipWithIndex().filter(lambda x: x[1] < 1))
    topVolume.pprint()

    ssc.start()
    ssc.awaitTermination()

