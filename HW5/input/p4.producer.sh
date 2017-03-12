#!/bin/bash

if [ $# -lt 2 ] ;  
then
        echo "Usage: $0 <input-file> <chunk-size>"
        exit 1
fi

FILE=$1
CHUNK_SIZE=$2

# provide an option to stop after sending n files.
NUM_FILES=999999
if [ ! -z "$3" ] ;
then
    NUM_FILES=$3
fi

# split the file into 1000 lines each
split -l $CHUNK_SIZE $FILE chunk 

count=0
for f in `ls chunk*`; do
    echo "file: $f"

    # cat the contents onto kafka producer on the topic 'mytopic'
    cat $f | kafka-console-producer.sh --broker-list localhost:9092 --topic mytopic
    sleep 1

    # check if we are done sending required number of files (optional)
    count=`expr $count + 1`
    if [ $count -eq $NUM_FILES ] ;
    then
        exit 1
    fi
done
