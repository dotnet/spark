#!/usr/bin/env bash

set -o errexit   # abort on nonzero exitstatus
set -o nounset   # abort on unbound variable
set -o pipefail  # don't hide errors within pipes

if [ -z "${SPARK_MASTER_URL}" ]; then
    "${SPARK_HOME}"/sbin/start-slave.sh spark://`hostname`:7077
else
    "${SPARK_HOME}"/sbin/start-slave.sh "${SPARK_MASTER_URL}"
fi
