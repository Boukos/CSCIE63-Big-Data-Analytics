#
# HW6 P2: Demonstrate linear regression using categorical and numerical variables as features
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

print "Mapping of categorical feature (Cylinders): %s" % get_mapping(records, 1)
print "Mapping of categorical feature (Model Year): %s" % get_mapping(records, 6)
print "Mapping of categorical feature (Origin): %s" % get_mapping(records, 7)
print "Mapping of categorical feature (Car Name): %s" % get_mapping(records, 8)

mappings = [get_mapping(records, i) for i in [1, 6, 7, 8]] # categorical variable index - 1, 6, 7, 8
cat_len = sum(map(len, mappings))
num_len = len(records.first()[2:6])  # numerical variable index - 2,3,4,5
total_len = num_len + cat_len

print "Feature vector length for categorical variables: %d" % cat_len
print "Feature vector length for numerical variables: %d" % num_len
print "Total feature vector length: %d" %total_len

from pyspark.mllib.regression import LabeledPoint
import numpy as np
def extract_features(record) :
    cat_vec = np.zeros(cat_len)
    i = 0
    step = 0
    cat_fields = [record[1], record[6], record[7], record[8]]   # 4 categorical fields
    for field in cat_fields:
        m = mappings[i]
        idx = m[field]  # get the idx associated with this categorical field value of input record
        cat_vec[idx + step] = 1
        i = i+1
        step = step + len(m)

    # convert 'NA' to zero in numerical variables
    j = 0
    num_fields = [record[2], record[3], record[4], record[5]]   # 4 numerical fields
    for field in num_fields:
        if field == "NA":
            num_fields[j] = 0
        j = j+1

    num_vec = np.array([float(field) for field in num_fields])
    return np.concatenate((cat_vec, num_vec))

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
print "First raw record (features): " + str(first[1:])
print "First label: " + str(first_point.label)
print "Linear model feature vector: " + str(first_point.features)
print "Linear model feature vector length: " + str(len(first_point.features))

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

# build regression model using training data
# predict mpg using a number of car features
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
