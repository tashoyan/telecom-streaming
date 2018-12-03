#!/bin/sh

set -o nounset
set -o errexit
set -o pipefail

input_dir="/stream/input"
checkpoint_dir="/stream/checkpoint-event-generator"
event_schema_file="/stream/event_schema.parquet"
kafka_brokers="ossv147:9092"

jar_file="$(ls generator/target/generator-*.jar | grep -vi javadoc || true)"
if test -z "$jar_file"
then
    echo "Cannot find the generator jar file. Is the project built? Exiting."
    exit 1
fi

hdfs dfs -test -e "$checkpoint_dir" && hdfs dfs -rm -r -skipTrash "$checkpoint_dir"
hdfs dfs -test -e "$input_dir" && hdfs dfs -rm -r -skipTrash "$input_dir"
hdfs dfs -test -e "$event_schema_file" && hdfs dfs -rm -r -skipTrash "$event_schema_file"
hdfs dfs -mkdir -p "$input_dir"
hdfs dfs -put "sampler/target/event_schema.parquet" "$(dirname $event_schema_file)"
hdfs dfs -ls "$input_dir"/../

spark-submit \
--master yarn \
--deploy-mode cluster \
--conf spark.yarn.maxAppAttempts=1 \
--conf spark.executor.instances=10 \
--conf spark.sql.shuffle.partitions=10 \
--class com.github.tashoyan.telecom.generator.EventGeneratorMain \
"$jar_file" \
--kafka-brokers "$kafka_brokers" \
--kafka-topic events \
--schema-file "$event_schema_file" \
--input-dir "$input_dir" \
--checkpoint-dir "$checkpoint_dir"
