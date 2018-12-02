#!/bin/sh

set -o nounset
set -o errexit
set +o pipefail

input_dir="/stream/input"
checkpoint_dir="/stream/checkpoint-event-generator"

jar_file="$(ls generator/target/generator-*.jar | grep -vi javadoc || true)"
if test -z "$jar_file"
then
    echo "Cannot find the generator jar file. Is the project built? Exiting."
    exit 1
fi

hdfs dfs -test -e "$checkpoint_dir" && hdfs dfs -rm -r -skipTrash "$checkpoint_dir"
hdfs dfs -test -e "$input_dir" && hdfs dfs -rm -r -skipTrash "$input_dir"
hdfs dfs -mkdir -p "$input_dir"
hdfs dfs -ls "$input_dir"/../

spark-submit \
--master yarn \
--deploy-mode cluster \
--conf spark.yarn.maxAppAttempts=1 \
--conf spark.executor.instances=10 \
--conf spark.sql.shuffle.partitions=10 \
--class com.github.tashoyan.telecom.generator.EventGeneratorMain \
event-producer-1.0.0-SNAPSHOT.jar \
--kafka-brokers localhost:9092 \
--kafka-topic events \
--input-dir "$input_dir" \
--checkpoint-dir "$checkpoint_dir"
