#!/bin/bash

# Usage: run1.sh <percent>
# where:
# - percent of NPIs to use (integer number, e.g., 10, 50, 100%)

if [[ -z $1 ]]; then
	pct="100"
else
	pct="$1"
fi

raw_data_file="raw_data$pct.txt"
hdfs_folder="medicare_$pct"

# prepare data for processing on Hadoop
python code/prep-data.py "data/$raw_data_file" $pct
hadoop fs -rm -r "$hdfs_folder"
hadoop fs -mkdir "$hdfs_folder"
hadoop fs -put "data/$raw_data_file" "$hdfs_folder/raw_data"

# Run pre-process to compute similarity and generate graph structure using PIG
pig -param folder=$hdfs_folder code/gen-graph.pig

