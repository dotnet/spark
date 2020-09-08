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
curl -k -L -o spark-2.3.1.tgz https://archive.apache.org/dist/spark/spark-2.3.1/spark-2.3.1-bin-hadoop2.7.tgz && tar xzvf spark-2.3.1.tgz
curl -k -L -o spark-2.3.2.tgz https://archive.apache.org/dist/spark/spark-2.3.2/spark-2.3.2-bin-hadoop2.7.tgz && tar xzvf spark-2.3.2.tgz
curl -k -L -o spark-2.3.3.tgz https://archive.apache.org/dist/spark/spark-2.3.3/spark-2.3.3-bin-hadoop2.7.tgz && tar xzvf spark-2.3.3.tgz
curl -k -L -o spark-2.3.4.tgz https://archive.apache.org/dist/spark/spark-2.3.4/spark-2.3.4-bin-hadoop2.7.tgz && tar xzvf spark-2.3.4.tgz
curl -k -L -o spark-2.4.0.tgz https://archive.apache.org/dist/spark/spark-2.4.0/spark-2.4.0-bin-hadoop2.7.tgz && tar xzvf spark-2.4.0.tgz
curl -k -L -o spark-2.4.1.tgz https://archive.apache.org/dist/spark/spark-2.4.1/spark-2.4.1-bin-hadoop2.7.tgz && tar xzvf spark-2.4.1.tgz
curl -k -L -o spark-2.4.3.tgz https://archive.apache.org/dist/spark/spark-2.4.3/spark-2.4.3-bin-hadoop2.7.tgz && tar xzvf spark-2.4.3.tgz
curl -k -L -o spark-2.4.4.tgz https://archive.apache.org/dist/spark/spark-2.4.4/spark-2.4.4-bin-hadoop2.7.tgz && tar xzvf spark-2.4.4.tgz
curl -k -L -o spark-2.4.5.tgz https://archive.apache.org/dist/spark/spark-2.4.5/spark-2.4.5-bin-hadoop2.7.tgz && tar xzvf spark-2.4.5.tgz
curl -k -L -o spark-2.4.6.tgz https://archive.apache.org/dist/spark/spark-2.4.6/spark-2.4.6-bin-hadoop2.7.tgz && tar xzvf spark-2.4.6.tgz
curl -k -L -o spark-3.0.0.tgz https://archive.apache.org/dist/spark/spark-3.0.0/spark-3.0.0-bin-hadoop2.7.tgz && tar xzvf spark-3.0.0.tgz

endlocal
