#!/bin/sh

set -o nounset
set -o errexit
set -o pipefail

topology_file="/stream/topology-correlator.parquet"
kafka_brokers="$(hostname):9092"
kafka_event_topic="events"
kafka_alarm_topic="alarms"
checkpoint_dir="/stream/checkpoint-correlator"
watermark_interval_millis=$((2 * 60 * 1000))
window_size_millis=$((60 * 1000))
window_slide_millis=$((30 * 1000))

jar_file="$(ls correlator/target/correlator-*.jar | grep -vi javadoc || true)"
if test -z "$jar_file"
then
    echo "Cannot find the application jar file. Is the project built? Exiting."
    exit 1
fi

export SPARK_DIST_CLASSPATH=$(hadoop classpath)

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
--conf spark.executor.instances=2 \
--conf spark.executor.cores=2 \
--conf spark.executor.memory=2g \
--conf spark.sql.shuffle.partitions=20 \
--conf spark.driver.memory=1g \
--conf spark.driver.cores=2 \
--class com.github.tashoyan.telecom.correlator.EventCorrelatorMain \
"$jar_file" \
--topology-file "$topology_file" \
--kafka-brokers "$kafka_brokers" \
--kafka-event-topic "$kafka_event_topic" \
--kafka-alarm-topic "$kafka_alarm_topic" \
--checkpoint-dir "$checkpoint_dir" \
--watermark-interval-millis "$watermark_interval_millis" \
--window-size-millis "$window_size_millis" \
--window-slide-millis "$window_slide_millis"
