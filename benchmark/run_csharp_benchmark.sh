#!/bin/bash

NUM_EXECUTORS=$1
DRIVER_MEMORY=$2
EXECUTOR_MEMORY=$3
EXECUTOR_CORES=$4
CSHARP_DLL=$5
JAR_PATH=$6
CSHARP_EXECUTABLE=$7
DATA_PATH=$8
NUM_ITERATION=$9
IS_SQL=$10

for i in {1..22}
do
  $SPARK_HOME/bin/spark-submit --master yarn --num-executors $NUM_EXECUTORS --driver-memory $DRIVER_MEMORY --executor-memory $EXECUTOR_MEMORY --executor-cores $EXECUTOR_CORES --files $CSHARP_DLL --class org.apache.spark.deploy.DotnetRunner $JAR_PATH $CSHARP_EXECUTABLE $DATA_PATH $i $NUM_ITERATION $IS_SQL
done
