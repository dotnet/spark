#!/bin/bash

COLD_ITERATION=$1
NUM_EXECUTORS=$2
DRIVER_MEMORY=$3
EXECUTOR_MEMORY=$4
EXECUTOR_CORES=$5
JAR_PATH=$6
DATA_PATH=$7
NUM_ITERATION=$8
IS_SQL=$9

for i in {1..22}
do
  for j in $(seq 1 $COLD_ITERATION)
  do
    $SPARK_HOME/bin/spark-submit --master yarn --num-executors $NUM_EXECUTORS --driver-memory $DRIVER_MEMORY --executor-memory $EXECUTOR_MEMORY --executor-cores $EXECUTOR_CORES --class com.microsoft.tpch.App $JAR_PATH $DATA_PATH $i $NUM_ITERATION $IS_SQL
  done
done
