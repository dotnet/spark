#!/bin/bash

##############################################################################
# Description:
# This script installs the worker binaries and your app dependencies onto
# your Databricks Spark cluster.
#
# Usage:
# Set DOTNET_SPARK_RELEASE in the cluster environment, or set it below, to the
# HTTPS URL of the Linux Worker archive matching your application's release.
#
##############################################################################
################################# CHANGE THESE ###############################

# Select the framework, architecture, and release from https://github.com/dotnet/spark/releases.
# Keep the Worker, Microsoft.Spark NuGet package, and bridge release aligned.
DOTNET_SPARK_RELEASE="${DOTNET_SPARK_RELEASE:-}"

# No need to change this unless you choose to use a different location
DBFS_INSTALLATION_ROOT=/dbfs/spark-dotnet
DOTNET_SPARK_WORKER_INSTALLATION_PATH=/usr/local/bin

###############################################################################

set -e
if [[ -z "$DOTNET_SPARK_RELEASE" || "$DOTNET_SPARK_RELEASE" == *[[:space:]]* ]]; then
    echo >&2 "Set DOTNET_SPARK_RELEASE to the HTTPS URL of the Linux Microsoft.Spark.Worker .tar.gz release matching your application."
    exit 1
fi

case "$DOTNET_SPARK_RELEASE" in
    https://*/Microsoft.Spark.Worker.*.linux-*-*.tar.gz) ;;
    *)
        echo >&2 "DOTNET_SPARK_RELEASE must name a Linux Worker .tar.gz archive from the selected release; copy its download URL from https://github.com/dotnet/spark/releases."
        exit 1
        ;;
esac

if [[ ! -r "$DBFS_INSTALLATION_ROOT/install-worker.sh" ]]; then
    echo >&2 "Upload install-worker.sh to $DBFS_INSTALLATION_ROOT before running this init script."
    exit 1
fi

/bin/bash "$DBFS_INSTALLATION_ROOT/install-worker.sh" github "$DOTNET_SPARK_RELEASE" "$DOTNET_SPARK_WORKER_INSTALLATION_PATH"



##############################################################################
# Uncomment below to deploy application dependencies to workers if submitting
# jobs using the "Set Jar" task (https://docs.databricks.com/user-guide/jobs.html#jar-jobs)
# Change the variables below appropriately
##############################################################################
################################# CHANGE THESE ###############################

#APP_DEPENDENCIES=/dbfs/apps/dependencies
#WORKER_PATH=`readlink $DOTNET_SPARK_WORKER_INSTALLATION_PATH/Microsoft.Spark.Worker`
#if [ -f $WORKER_PATH ] && [ -d $APP_DEPENDENCIES ]; then
#    sudo cp -fR $APP_DEPENDENCIES/. `dirname $WORKER_PATH`
#fi
