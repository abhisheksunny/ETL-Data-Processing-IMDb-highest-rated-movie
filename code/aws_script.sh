#!/bin/sh

code_location=/home/hadoop/code

mkdir -p $code_location
cd $code_location

aws s3 sync s3://udacity-dend-capstone-code-ab/ $code_location
pwd

export SPARK_HOME=/usr/lib/spark
export PYTHONPATH=$SPARK_HOME/python/lib/py4j-0.10.7-src.zip:$PYTHONPATH
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/build:$PYTHONPATH

export PYSPARK_PYTHON=/usr/bin/python3
export PYSPARK_DRIVER_PYTHON=$PYSPARK_PYTHON

sudo python3 -m pip install --upgrade pip
sudo python3 -m pip install configparser

python3 $code_location/main.py

