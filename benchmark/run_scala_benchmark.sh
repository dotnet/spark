#!/bin/bash

NUM_EXECUTORS=$1
DRIVER_MEMORY=$2
EXECUTOR_MEMORY=$3
EXECUTOR_CORES=$4
JAR_PATH=$5
DATA_PATH=$6
NUM_ITERATION=$7
IS_SQL=$8

for i in {1..22}
do
  $SPARK_HOME/bin/spark-submit --master yarn --num-executors $NUM_EXECUTORS --driver-memory $DRIVER_MEMORY --executor-memory $EXECUTOR_MEMORY --executor-cores $EXECUTOR_CORES --class com.microsoft.tpch.App $JAR_PATH $DATA_PATH $i $NUM_ITERATION $IS_SQL
done
