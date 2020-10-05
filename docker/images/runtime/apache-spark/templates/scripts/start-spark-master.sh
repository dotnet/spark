#!/usr/bin/env bash

set -o errexit   # abort on nonzero exitstatus
set -o nounset   # abort on unbound variable
set -o pipefail  # don't hide errors within pipes

if [ -z "${SPARK_MASTER_DISABLED}" ]; then
    "${SPARK_HOME}"/sbin/start-master.sh
fi
