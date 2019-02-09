#!/bin/sh

set -o nounset
set -o errexit
set -o pipefail

kafka_brokers="$(hostname):9092"
kafka_topic="events"
checkpoint_dir="/stream/checkpoint-writer"
output_dir="/stream/output"
watermark_interval_sec=600

jar_file="$(ls writer/target/writer-*.jar | grep -vi javadoc || true)"
if test -z "$jar_file"
then
    echo "Cannot find the application jar file. Is the project built? Exiting."
    exit 1
fi

hdfs dfs -test -e "$checkpoint_dir" && hdfs dfs -rm -r -skipTrash "$checkpoint_dir"
hdfs dfs -test -e "$output_dir" && hdfs dfs -rm -r -skipTrash "$output_dir"
hdfs dfs -mkdir -p "$output_dir"
hdfs dfs -ls "$output_dir"/../

app_name="$(basename $0)"
spark-submit \
--name "$app_name" \
--master yarn \
--deploy-mode cluster \
--conf spark.yarn.maxAppAttempts=1 \
--conf spark.executor.instances=5 \
--conf spark.sql.shuffle.partitions=5 \
--class com.github.tashoyan.telecom.writer.EventWriterMain \
"$jar_file" \
--kafka-brokers "$kafka_brokers" \
--kafka-topic "$kafka_topic" \
--checkpoint-dir "$checkpoint_dir" \
--output-dir "$output_dir" \
--watermark-interval-sec "$watermark_interval_sec"
