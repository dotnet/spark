#!/bin/bash
set +e

# Cloud Provider
CLOUD_PROVIDER=$1
# Path where packaged worker file (tgz) exists.
WORKER_PATH=$2
# The path on the executor nodes where Microsoft.Spark.Worker executable is installed.
DEST_PATH=$3
# The path where all the dependent libraies are installed so that it doesn't
# pollute the $DEST_PATH.
DEST_PATH_BINARIES=$DEST_PATH/microsoft.spark.worker
# Temporary worker file.
TEMP_WORKER_FILENAME=/tmp/temp_worker.tgz

# Clean up any existing files. 
sudo rm -f $DEST_PATH/Microsoft.Spark.Worker
sudo rm -rf $DEST_PATH_BINARIES

# Copy the worker file to a local temporary file.
if [ "${CLOUD_PROVIDER,,}" = "azure" ]; then
  hdfs dfs -get $WORKER_PATH $TEMP_WORKER_FILENAME
elif [ "${CLOUD_PROVIDER,,}" = "aws" ]; then
  aws s3 cp $WORKER_PATH $TEMP_WORKER_FILENAME
else
  cp -f $WORKER_PATH $TEMP_WORKER_FILENAME
fi

# Untar the file.
sudo mkdir -p $DEST_PATH_BINARIES
sudo tar xzf $TEMP_WORKER_FILENAME -C $DEST_PATH_BINARIES

# Make the file executable since dotnet doesn't set this correctly.
sudo chmod 755 $DEST_PATH_BINARIES/Microsoft.Spark.Worker

# Create a symlink.
sudo ln -sf $DEST_PATH_BINARIES/Microsoft.Spark.Worker $DEST_PATH/Microsoft.Spark.Worker

# Remove the temporary worker file.
sudo rm $TEMP_WORKER_FILENAME
