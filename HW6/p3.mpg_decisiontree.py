#
# HW6 P3: Demonstrate decision tree model using categorical and numerical variables as features
# Input: auto_mpg_original.csv
#
from pyspark import SparkConf, SparkContext

# initialize streaming context
conf = SparkConf().setMaster("local[2]").setAppName("AutoRegression")
sc = SparkContext(conf = conf)

# load csv file
path="file:///home/cloudera/CSCIE63/HW6/auto_mpg_original.csv"
raw_data=sc.textFile(path)
total_size=raw_data.count()
records=raw_data.map(lambda x: x.split(","))
# cache records
records.cache()

# mapping of values with unique index for a categorical variable
def get_mapping(rdd, idx):
    return rdd.map(lambda fields: fields[idx]).distinct().zipWithIndex().collectAsMap()

print "Mapping of categorical feature (Car Name): %s" % get_mapping(records, 8)
mappings_car_name=get_mapping(records, 8)

def mapNA(field):
    if field == "NA":
        return 0
    return field

from pyspark.mllib.regression import LabeledPoint
import numpy as np
def extract_features(record) :
    feature_vec = np.array([float(mapNA(field)) for field in record[1:8]])
    # map car-name (string) to a distinct integer value
    feature_car_name = np.array([float(mappings_car_name[record[8]])])
    return np.concatenate((feature_vec, feature_car_name))

def extract_label(record) :
    if record[0] == "NA":
        return 0.0
    return float(record[0])   # target variable (mpg) index

# calculate squared_log value (for RMSLE purpose) given a predicted and actual value
def squared_log_error(pred, actual):
    return (np.log(pred + 1) - np.log(actual + 1)) ** 2

# calcualte squared value (for RMSE purpose) given a predicted and actual value
def squared_error(pred, actual):
    return (pred - actual) ** 2

# calculate absolute val (for MAE purpose) given a predicted and actual value
def absolute_error(pred, actual):
    return (np.abs(pred - actual))

# build extracted feature RDD
data = records.map(lambda r: LabeledPoint(extract_label(r), extract_features(r)))

#inspect first record in the extracted feature RDD
first = records.first()
first_point = data.first()
print "First raw record: " + str(first[1:])
print "First Label: " + str(first_point.label)
print "Decision tree feature vector: " + str(first_point.features)
print "Decision tree feature vector length: " + str(len(first_point.features))

# create test and train data
data_with_idx = data.zipWithIndex().map(lambda (k, v): (v, k))
test = data_with_idx.sample(False, 0.2, 42)
train = data_with_idx.subtractByKey(test)

train_data = train.map(lambda (idx, p) : p)
test_data = test.map(lambda (idx, p) : p)
train_size = train_data.count()
test_size = test_data.count()

print "Training data size: %d" % train_size
print "Test data size: %d" % test_size
print "Total data size: %d" % total_size

# build decision tree model using training data
# predict mpg using a number of car features.
from pyspark.mllib.tree import DecisionTree
dt_model = DecisionTree.trainRegressor(train_data, {})
print "Decision tree model: ", dt_model

# test the decision tree model using test data
preds = dt_model.predict(test_data.map(lambda p: p.features))
actual = test_data.map(lambda p: p.label)
true_vs_predicted = actual.zip(preds)
print "Decision Tree predictions: " + str(true_vs_predicted.take(5))
print "Decision Tree depth: " + str(dt_model.depth())
print "Desicion Tree number of nodes: " + str(dt_model.numNodes())

# measure the accuracy of the model using RMSE - root mean square error
accuracy_rmse = np.sqrt(true_vs_predicted.map(lambda (t, p): squared_error(t, p)).mean())
print "Model accuracy using RMSE measure: %2.4f" % accuracy_rmse

# measure the accuracy of the model using MAE - Mean Absolute error
accuracy_mae = true_vs_predicted.map(lambda (t, p): absolute_error(t, p)).mean()
print "Model accuracy using MAE measure: %2.4f" % accuracy_mae

# measure the accuracy of the model using RMSLE - root mean square log error
accuracy_rmsle = np.sqrt(true_vs_predicted.map(lambda (t, p): squared_log_error(t,p)).mean())
print "Model accuracy using RMSLE measure: %2.4f" % accuracy_rmsle
