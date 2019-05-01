#!/bin/bash

NUGET_DIR=$1
WORKER_DIR=$2
OUTPUT_DIR=$3

VERSION=`ls ${NUGET_DIR}/Microsoft.Spark.*.nupkg | head -n 1 | xargs -n 1 basename | cut -d'.' -f3-5`
TEMP_WORKER_DIR=Microsoft.Spark.Worker-${VERSION}

mkdir -p ${OUTPUT_DIR}

for FRAMEWORK in `ls ${WORKER_DIR}`
do
  for RUNTIME in `ls ${WORKER_DIR}/${FRAMEWORK}`
  do
    mkdir -p ${TEMP_WORKER_DIR}
    cp -R ${WORKER_DIR}/${FRAMEWORK}/${RUNTIME}/* ${TEMP_WORKER_DIR}
    FILENAME=Microsoft.Spark.Worker.${FRAMEWORK}.${RUNTIME}-${VERSION}

    if [ "${RUNTIME,,}" = "linux-x64" ]; then
      chmod 755 ${TEMP_WORKER_DIR}/Microsoft.Spark.Worker 
      tar czf ${OUTPUT_DIR}/${FILENAME}.tar.gz ${TEMP_WORKER_DIR}
    else
      zip -r ${OUTPUT_DIR}/${FILENAME}.zip ${TEMP_WORKER_DIR}
    fi

    # Clean up temp worker dir
    rm -rf ${TEMP_WORKER_DIR}
  done
done
