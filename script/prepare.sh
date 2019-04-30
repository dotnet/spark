#!/bin/bash

set +e

VERSION=$1
WORKER_DIR=Microsoft.Spark.Worker-$VERSION

rm -rf ./Microsoft.Spark.Binaries/
unzip ./Microsoft.Spark.Binaries.zip

rm -rf $WORKER_DIR
mkdir $WORKER_DIR
cp ./Microsoft.Spark.Binaries/Microsoft.Spark.Worker/net461/win-x64/* $WORKER_DIR
zip -r Microsoft.Spark.Worker.net461.win-x64-$VERSION.zip $WORKER_DIR

rm -rf $WORKER_DIR
mkdir $WORKER_DIR
cp ./Microsoft.Spark.Binaries/Microsoft.Spark.Worker/netcoreapp2.1/win-x64/* $WORKER_DIR
zip -r Microsoft.Spark.Worker.netcoreapp2.1.win-x64-$VERSION.zip $WORKER_DIR

rm -rf $WORKER_DIR
mkdir $WORKER_DIR
cp ./Microsoft.Spark.Binaries/Microsoft.Spark.Worker/netcoreapp2.1/linux-x64/* $WORKER_DIR
tar czf Microsoft.Spark.Worker.netcoreapp2.1.linux-x64-$VERSION.tar.gz $WORKER_DIR

rm -rf $WORKER_DIR
