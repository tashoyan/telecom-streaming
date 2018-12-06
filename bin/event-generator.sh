#!/bin/sh

set -o nounset
set -o errexit
set -o pipefail

event_schema_file="/stream/event-schema-generator.parquet"
input_dir="/stream/input"
kafka_brokers="ossv147:9092"
kafka_topic="events"
checkpoint_dir="/stream/checkpoint-generator"

jar_file="$(ls generator/target/generator-*.jar | grep -vi javadoc || true)"
if test -z "$jar_file"
then
    echo "Cannot find the application jar file. Is the project built? Exiting."
    exit 1
fi

hdfs dfs -test -e "$checkpoint_dir" && hdfs dfs -rm -r -skipTrash "$checkpoint_dir"
hdfs dfs -test -e "$input_dir" && hdfs dfs -rm -r -skipTrash "$input_dir"
hdfs dfs -test -e "$event_schema_file" && hdfs dfs -rm -r -skipTrash "$event_schema_file"
hdfs dfs -mkdir -p "$input_dir"
hdfs dfs -put "sampler/target/event_schema.parquet" "$event_schema_file"
hdfs dfs -ls "$input_dir"/../

#app_name="$(basename $0)"
app_name="Text Labeling 1.1.1SNAPSHOT"
spark-submit \
--name "$app_name" \
--master yarn \
--deploy-mode cluster \
--conf spark.yarn.maxAppAttempts=1 \
--conf spark.executor.instances=5 \
--conf spark.sql.shuffle.partitions=5 \
--class com.github.tashoyan.telecom.generator.EventGeneratorMain \
"$jar_file" \
--schema-file "$event_schema_file" \
--input-dir "$input_dir" \
--kafka-brokers "$kafka_brokers" \
--kafka-topic "$kafka_topic" \
--checkpoint-dir "$checkpoint_dir"
