from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, Row

# init spark context
conf = SparkConf().setMaster("local").setAppName("Employees")
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)

# load from parquet file
print "------------------Load empl data from parquet file--------------------\n"
empsParquet = sqlContext.read.parquet("empl.parquet")
empsParquet.registerTempTable("empsTemp")
empsParquetDF = sqlContext.sql("select name, age, salary from empsTemp")
empsParquetDF.show()

