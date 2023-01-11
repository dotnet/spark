#!/bin/bash

while sudo fuser /var/{lib/{dpkg,apt/lists},cache/apt/archives}/lock >/dev/null 2>&1 ; do
     echo 'Waiting for release of dpkg locks'
     sleep 1
done
