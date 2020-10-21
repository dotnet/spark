#!/usr/bin/env bash

# Copy jar to the current working directory, if it does not exist already
jar_file_dest="$(pwd)/microsoft-spark-X.X.X.jar"
if ! [ -f "${jar_file_dest}" ]; then
    cp "${HOME}/microsoft-spark-X.X.X.jar" "$(pwd)"
fi

# Start the .NET for Apache Spark backend in debug mode
running=$(: &>/dev/null </dev/tcp/127.0.0.1/5567 && echo "true" || echo "false")

if [ "${running}" = "false" ]; then
    /spark/bin/spark-submit --class org.apache.spark.deploy.dotnet.DotnetRunner --jars "$(pwd)/*.jar" --master local microsoft-spark-X.X.X.jar debug 5567
else
    echo ".NET Backend is running in debug mode, already"
fi
