#!/bin/bash

if [ $# -ne 2 ]; then
echo "Invalid number of parameters!"
echo "Usage: ./tag_driver_combiner.sh [input_location] [output_location]"
exit 1
fi

hadoop jar /usr/lib/hadoop/hadoop-streaming-2.8.5-amzn-1.jar \
-D mapreduce.job.reduces=3 \
-D mapreduce.job.name='COMP5349_assignment1' \
-file category_mapper.py \
-mapper 'python category_mapper.py' \
-file category_reducer.py \
-reducer 'python category_reducer.py' \
-input $1 \
-output $2
