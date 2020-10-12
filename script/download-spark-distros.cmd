@echo off

setlocal

set OutputDir=%1
set SparkVersion=%2

cd %OutputDir%

echo "Downloading Spark distros."

curl -k -L -o spark-%SparkVersion%.tgz https://archive.apache.org/dist/spark/spark-%SparkVersion%/spark-%SparkVersion%-bin-hadoop2.7.tgz && tar xzvf spark-%SparkVersion%.tgz

endlocal
