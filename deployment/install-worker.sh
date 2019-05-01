#!/bin/bash

##############################################################################
# Description:
# This is a helper script to install the worker binaries on your Apache Spark cluster
#
# Usage:
# ./install-script.sh <cloud-provider> <path-to-worker> <local-worker-installation-path>
#
# Sample usage:
# ./install-script.sh 
#   azure 
#   abfs://<blobcontainer>@<gen2storageaccount>.dfs.core.windows.net/<path>/Microsoft.Spark.Worker.netcoreapp2.1.linux-x64-0.1.0.tar.gz 
#   /usr/local/bin
##############################################################################

set +e
# Cloud Provider
CLOUD_PROVIDER=$1

# Path where packaged worker file (tgz) exists.
WORKER_PATH_OR_URI=$2

# The path on the executor nodes where Microsoft.Spark.Worker executable is installed.
LOCAL_WORKER_PATH=$3

# The path where all the dependent libraies are installed so that it doesn't
# pollute the $LOCAL_WORKER_PATH.
SPARKDOTNET_ROOT=$LOCAL_WORKER_PATH/spark-dot-net

# Temporary worker file.
TEMP_WORKER_FILENAME=/tmp/temp_worker.tgz

# Extract version
IFS='-' read -ra BASE_FILENAME <<< "$(basename $WORKER_PATH_OR_URI .tar.gz)"
VERSION=${BASE_FILENAME[2]}

IFS='.' read -ra VERSION_CHECK <<< "$VERSION"
[[ ${#VERSION[@]} != 3 ]] || { echo >&2 "Version check does not satisfy. Raise an issue here: https://github.com/dotnet/spark"; exit 1; }

# Path of the final worker binaries (the one we just downloaded and extracted)
CURRENT_WORKER_PATH=$SPARKDOTNET_ROOT/Microsoft.Spark.Worker-$VERSION
CURRENT_WORKER_BINARY=$CURRENT_WORKER_PATH/Microsoft.Spark.Worker

# Clean up any existing files.
sudo rm -f $LOCAL_WORKER_PATH/Microsoft.Spark.Worker
sudo rm -rf $SPARKDOTNET_ROOT

# Copy the worker file to a local temporary file.
if [ "${CLOUD_PROVIDER,,}" = "azure" ]; then
  hdfs dfs -get $WORKER_PATH_OR_URI $TEMP_WORKER_FILENAME
elif [ "${CLOUD_PROVIDER,,}" = "aws" ]; then
  aws s3 cp $WORKER_PATH_OR_URI $TEMP_WORKER_FILENAME
else
  cp -f $WORKER_PATH_OR_URI $TEMP_WORKER_FILENAME
fi

# Untar the file.
sudo mkdir -p $SPARKDOTNET_ROOT
sudo tar xzf $TEMP_WORKER_FILENAME -C $SPARKDOTNET_ROOT

# Make the file executable since dotnet doesn't set this correctly.
sudo chmod 755 $CURRENT_WORKER_BINARY

# Create a symlink.
sudo ln -sf $CURRENT_WORKER_BINARY $LOCAL_WORKER_PATH/Microsoft.Spark.Worker

# Remove the temporary worker file.
sudo rm $TEMP_WORKER_FILENAME
