#!/bin/bash

if [ $# -lt 1 ] ;
then
    echo "Usage: $0 <dir path>"
    exit 1
fi

set -x

hadoop fs -rm -f $1/input/*
hadoop fs -rm -f $1/staging/*
hadoop fs -rm -r -f $1/ouput/output*

