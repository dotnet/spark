#!/usr/bin/env bash

# Start the .NET for Apache Spark backend in debug mode

cd /dotnet/Debug/netcoreapp3.1  || exit
/spark/bin/spark-submit --class org.apache.spark.deploy.dotnet.DotnetRunner --jars "/dotnet/Debug/netcoreapp3.1/*.jar" --master local microsoft-spark-2.4.x-0.12.1.jar debug 5567
