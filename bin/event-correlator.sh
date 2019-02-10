#!/bin/sh

set -o nounset
set -o errexit
set -o pipefail

topology_file="/stream/topology-correlator.parquet"
kafka_brokers="$(hostname):9092"
kafka_event_topic="events"
kafka_alarm_topic="alarms"
checkpoint_dir="/stream/checkpoint-correlator"
watermark_interval_sec=600
window_size_sec=60
window_shift_sec=30

jar_file="$(ls correlator/target/correlator-*.jar | grep -vi javadoc || true)"
if test -z "$jar_file"
then
    echo "Cannot find the application jar file. Is the project built? Exiting."
    exit 1
fi

hdfs dfs -test -e "$checkpoint_dir" && hdfs dfs -rm -r -skipTrash "$checkpoint_dir"
hdfs dfs -test -e "$topology_file" && hdfs dfs -rm -r -skipTrash "$topology_file"
hdfs dfs -put "resources/topology_controller_station.parquet" "$topology_file"
hdfs dfs -ls "$topology_file"/../

app_name="$(basename $0)"
spark-submit \
--name "$app_name" \
--master yarn \
--deploy-mode cluster \
--conf spark.yarn.maxAppAttempts=1 \
--conf spark.executor.instances=5 \
--conf spark.sql.shuffle.partitions=5 \
--class com.github.tashoyan.telecom.correlator.EventCorrelatorMain \
"$jar_file" \
--topology-file "$topology_file" \
--kafka-brokers "$kafka_brokers" \
--kafka-event-topic "$kafka_event_topic" \
--kafka-alarm-topic "$kafka_alarm_topic" \
--checkpoint-dir "$checkpoint_dir" \
--watermark-interval-sec "$watermark_interval_sec" \
--window-size-sec "$window_size_sec" \
--window-shift-sec "$window_shift_sec"
