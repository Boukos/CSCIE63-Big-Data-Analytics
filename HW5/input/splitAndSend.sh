#!/bin/bash

if [ -z "$1" ]; then
        echo "Missing output folder name"
        exit 1
fi

#FILE=test.txt

HDFS_DATA=/user/cloudera/CSCIE63/HW5/p2
#HDFS_DATA=/user/cloudera/CSCIE63/HW5/p1
FILE=orders.txt

split -l 10000  $FILE chunk 

for f in `ls chunk*`; do
        if [ "$2" == "local" ]; then
                mv $f $1
        else
                echo "$f ==> $HDFS_DATA/staging/"
                hadoop fs -put $f $HDFS_DATA/staging/
                echo "$HDFS_DATA/staging ==> $HDFS_DATA/input/"
                hadoop fs -mv $HDFS_DATA/staging/$f $HDFS_DATA/input/
                rm -f $f
        fi
        sleep 3 
done
