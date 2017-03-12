from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, Row

# init spark context
conf = SparkConf().setMaster("local").setAppName("Employees")
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)

# load employee file
empsFile = "/home/cloudera/CSCIE63/HW4/emps-1.txt"
emps = sc.textFile("file://"+empsFile)
print "------------------Loading employee file-------------------------------\n"
print emps.collect()

# tokenize each line on ','
emps_fields = emps.map(lambda line: line.split(","))
print "------------------Creating tuple from each row------------------------\n"
print emps_fields.collect()

# create rows of data
employees = emps_fields.map(lambda row: Row(name = row[0], age = int(row[1]), salary = float(row[2])))
print "------------------Creating rows of data-------------------------------\n"
print employees.collect()

# create a dataframe
empsDF = sqlContext.createDataFrame(employees)
print "------------------Creating dataframe----------------------------------\n"
print empsDF.show()

# show the schema
print "------------------Dataframe schema------------------------------------\n"
print empsDF.printSchema()

# show employees with salary > 3500
print "------------------Employess with salary > 3500------------------------\n"
print empsDF.filter(empsDF.salary > 3500).show()

# save in parquet file
print "------------------Saving as parquet file------------------------------\n"
print empsDF.select("name", "age", "salary").write.save("empl.parquet", format="parquet")
print "----------------------------------------------------------------------\n"

# load from parquet file
print "------------------Load empl data from parquet file--------------------\n"
empsParquet = sqlContext.read.parquet("empl.parquet")
empsParquet.registerTempTable("empsTemp")
empsParquetDF = sqlContext.sql("select name, age, salary from empsTemp")
empsParquetDF.show()

