@echo off

setlocal

set OutputDir=%1
cd %OutputDir%

echo "Download Hadoop binaries for Windows."
curl -k -L -o hadoop.zip https://github.com/steveloughran/winutils/releases/download/tag_2017-08-29-hadoop-2.8.1-native/hadoop-2.8.1.zip
unzip hadoop.zip
mkdir -p hadoop\bin
cp hadoop-2.8.1\winutils.exe hadoop\bin

echo "Downloading Spark distros."

curl -k -L -o spark-2.3.0.tgz https://archive.apache.org/dist/spark/spark-2.3.0/spark-2.3.0-bin-hadoop2.7.tgz && tar xzvf spark-2.3.0.tgz

endlocal