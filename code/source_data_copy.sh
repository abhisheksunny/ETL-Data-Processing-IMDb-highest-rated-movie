#!/bin/sh

source_dir=$1
source_dir_archived=$2
date=$3

source_data_local=/home/hadoop/source_data
mkdir -p $source_data_local
cd $source_data_local

wget https://datasets.imdbws.com/title.basics.tsv.gz 
wget https://datasets.imdbws.com/title.crew.tsv.gz
wget https://datasets.imdbws.com/title.ratings.tsv.gz
wget https://datasets.imdbws.com/name.basics.tsv.gz

hadoop fs -mkdir -p $source_dir

hadoop fs -put $source_data_local/* $source_dir

aws s3 cp $source_data_local $source_dir_archived/$date/ --recursive

