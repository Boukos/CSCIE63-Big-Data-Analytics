#
# HW6 P1: Demonstrate linear regression using one numerical variable as a feature
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


from pyspark.mllib.regression import LabeledPoint
import numpy as np

# return label (dependent variable) value
def extract_label(record):
    # Horsepower.
    # Not sure returning 0.0 for NA values is the best strategy?
    if record[3] == "NA":
        return 0.0
    return float(record[3])

# Extract features (independent variables) from record
def extract_features(record):
    # Displacement
    num_vec = np.array([float(record[2])])
    return num_vec

# calculate squared_log value (for RMSLE purpose) given a predicted and actual value
def squared_log_error(pred, actual):
    return (np.log(pred + 1) - np.log(actual + 1)) ** 2

# calcualte squared value (for RMSE purpose) given a predicted and actual value
def squared_error(pred, actual):
    return (pred - actual) ** 2

# calculate absolute val (for MAE purpose) given a predicted and actual value
def absolute_error(pred, actual):
    return (np.abs(pred - actual))

# build RDD of labeled points
first = records.first()
print "Sample raw data: " + str(first[0:])
data = records.map(lambda r:LabeledPoint(extract_label(r), extract_features(r)))
first_point = data.first()
print "Sample label (horsepower): " + str(first_point.label)
print "Sample feature (displacement): " + str(first_point.features)
print "Sample labeled points: ", data.take(5)

# create test and train data
data_with_idx = data.zipWithIndex().map(lambda (k, v): (v, k))
test = data_with_idx.sample(False, 0.2, 42)  # 0.2 means 20% test data
train = data_with_idx.subtractByKey(test)

train_data = train.map(lambda (idx, p) : p)
test_data = test.map(lambda (idx, p) : p)
train_size = train_data.count()
test_size = test_data.count()

print "Training data size: %d" % train_size
print "Test data size: %d" % test_size
print "Total data size: %d" % total_size

# build regression model using training data
# predict horsepower using displacement as feature.
from pyspark.mllib.regression import LinearRegressionWithSGD
linear_model = LinearRegressionWithSGD.train(train_data, iterations=200, step=0.000001, intercept=False)
print "Linear model parameters: ", linear_model

# test the regression model using test data
test_predict = test_data.map(lambda p: (p.label, linear_model.predict(p.features)))
print "Linear model predications (samples using test data): ", test_predict.take(5)

# measure the accuracy of the model using RMSE - root mean square error
accuracy_rmse = np.sqrt(test_predict.map(lambda (t, p): squared_error(t, p)).mean())
print "Model accuracy using RMSE measure: %2.4f" % accuracy_rmse

# measure the accuracy of the model using MAE - Mean Absolute error
accuracy_mae = test_predict.map(lambda (t, p): absolute_error(t, p)).mean()
print "Model accuracy using MAE measure: %2.4f" % accuracy_mae

# measure the accuracy of the model using RMSLE - root mean square log error
accuracy_rmsle = np.sqrt(test_predict.map(lambda (t, p): squared_log_error(t,p)).mean())
print "Model accuracy using RMSLE measure: %2.4f" % accuracy_rmsle

# plot regression line
from numpy import array
from pylab import plot, show

xdata = records.map(lambda r:float(r[2]))
xi = array(xdata.collect())
slope = linear_model.weights.values[0]
yi = slope * xi
plot(xi, yi, 'r-', xi, yi, 'o')
show()
