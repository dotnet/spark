#!/bin/bash

##############################################################################
# Description:
# This is a wrapper script to install the worker binaries on your Databricks Spark cluster.
# It exists only because Databricks does not allow parameters for an init-script.
#
# Usage:
# Change the variables below appropriately. 
#
##############################################################################
################################# CHANGE THESE ###############################

# DOTNET_SPARK_RELEASE to point to the appropriate version you downloaded from the
# https://github.com/dotnet/spark Releases section. For instance, for v0.2.0, you
# would set it to the following URI:
# https://github.com/dotnet/spark/releases/download/v0.2.0/Microsoft.Spark.Worker.netcoreapp2.1.linux-x64-0.2.0.tar.gz
DOTNET_SPARK_RELEASE=https://github.com/dotnet/spark/releases/download/v0.2.0/Microsoft.Spark.Worker.netcoreapp2.1.linux-x64-0.2.0.tar.gz

# No need to change this unless you choose to use a different location
DBFS_INSTALLATION_ROOT=/dbfs/spark-dotnet
DOTNET_SPARK_WORKER_INSTALLATION_PATH=/usr/local/bin

###############################################################################

set +e
/bin/bash $DBFS_INSTALLATION_ROOT/$DOTNET_SPARK_RELEASE github $DBFS_INSTALLATION_ROOT/$DOTNET_SPARK_RELEASE $DOTNET_SPARK_WORKER_INSTALLATION_PATH
