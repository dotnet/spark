#!/usr/bin/env bash

spark_short_version="${SPARK_VERSION:0:3}"
# Start the .NET for Apache Spark backend in debug mode
cd "${HOME}"/dotnet.spark  || exit
/spark/bin/spark-submit --class org.apache.spark.deploy.dotnet.DotnetRunner --jars "${HOME}/dotnet.spark/*.jar" --master local microsoft-spark-"${spark_short_version}".x-"${DOTNET_SPARK_VERSION}".jar debug 5567
