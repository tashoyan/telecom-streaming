#!/bin/sh

set -o nounset
set -o errexit
set -o pipefail

export HADOOP_CLASSPATH=$(hadoop classpath)

#TODO Real predictor app
app_name="$(basename $0)"
flink run \
-ynm "$app_name" \
-m yarn-cluster \
-p 10 \
-ys 2 \
-yjm 1G \
-ytm 2G \
./examples/batch/WordCount.jar \
--input hdfs:///ci_storage/output_FACT_OSI_NOTIF_EVENT_201901241200.dat
