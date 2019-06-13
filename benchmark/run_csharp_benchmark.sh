#!/bin/bash

COLD_ITERATION=$1
NUM_EXECUTORS=$2
DRIVER_MEMORY=$3
EXECUTOR_MEMORY=$4
EXECUTOR_CORES=$5
CSHARP_DLL=$6
JAR_PATH=$7
CSHARP_EXECUTABLE=$8
DATA_PATH=$9
NUM_ITERATION=${10}
IS_SQL=${11}

for i in {1..22}
do
  for j in $(seq 1 $COLD_ITERATION)
  do
    $SPARK_HOME/bin/spark-submit --master yarn --num-executors $NUM_EXECUTORS --driver-memory $DRIVER_MEMORY --executor-memory $EXECUTOR_MEMORY --executor-cores $EXECUTOR_CORES --files $CSHARP_DLL --class org.apache.spark.deploy.DotnetRunner $JAR_PATH $CSHARP_EXECUTABLE $DATA_PATH $i $NUM_ITERATION $IS_SQL
  done
done
