from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, HiveContext

# create HiveContext
conf = SparkConf().setMaster("local").setAppName("Customer")
sc = SparkContext(conf = conf)
HiveContext = SQLContext(sc)

# load data from Hive Customer table into a dataframe
custDF = hiveContext.sql("select * from Customers")
print "#rows in customer table: ", custDF.count()
print "first row in customer table: ", custDF.first()
print "customer table schema: ", custDF.printSchema()

# print #shipping addresses in each US state
print custDF.groupBy("customer_state").count().show()
