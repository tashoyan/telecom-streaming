#!/bin/sh

set -o nounset
set -o errexit
set -o pipefail

event_schema_file="/stream/event-schema-writer.parquet"
kafka_brokers="ossv147:9092"
kafka_topic="events"
checkpoint_dir="/stream/checkpoint-writer"
output_dir="/stream/output"

jar_file="$(ls writer/target/writer-*.jar | grep -vi javadoc || true)"
if test -z "$jar_file"
then
    echo "Cannot find the application jar file. Is the project built? Exiting."
    exit 1
fi

hdfs dfs -test -e "$checkpoint_dir" && hdfs dfs -rm -r -skipTrash "$checkpoint_dir"
hdfs dfs -test -e "$output_dir" && hdfs dfs -rm -r -skipTrash "$output_dir"
hdfs dfs -test -e "$event_schema_file" && hdfs dfs -rm -r -skipTrash "$event_schema_file"
hdfs dfs -mkdir -p "$output_dir"
hdfs dfs -put "sampler/target/event_schema.parquet" "$event_schema_file"
hdfs dfs -ls "$output_dir"/../

spark-submit \
--master yarn \
--deploy-mode cluster \
--conf spark.yarn.maxAppAttempts=1 \
--conf spark.executor.instances=5 \
--conf spark.sql.shuffle.partitions=5 \
--class com.github.tashoyan.telecom.writer.EventWriterMain \
"$jar_file" \
--schema-file "$event_schema_file" \
--kafka-brokers "$kafka_brokers" \
--kafka-topic "$kafka_topic" \
--checkpoint-dir "$checkpoint_dir" \
--output-dir "$output_dir"
