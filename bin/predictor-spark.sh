#!/bin/sh

set -o nounset
set -o errexit
set -o pipefail

kafka_brokers="$(hostname):9092"
kafka_event_topic="events"
kafka_alarm_topic="alarms"
checkpoint_dir="/stream/checkpoint-predictor-spark"
watermark_interval_millis=$((10 * 60 * 1000))
problem_timeout_millis=$((20 * 1000))

jar_file="$(ls predictor-spark/target/predictor-spark-*.jar | grep -vi javadoc || true)"
if test -z "$jar_file"
then
    echo "Cannot find the application jar file. Is the project built? Exiting."
    exit 1
fi

export SPARK_DIST_CLASSPATH=$(hadoop classpath)

hdfs dfs -test -e "$checkpoint_dir" && hdfs dfs -rm -r -skipTrash "$checkpoint_dir"

app_name="$(basename $0)"
spark-submit \
--name "$app_name" \
--master yarn \
--deploy-mode cluster \
--conf spark.yarn.maxAppAttempts=1 \
--conf spark.executor.instances=5 \
--conf spark.sql.shuffle.partitions=5 \
--conf spark.driver.memory=1g \
--conf spark.driver.cores=2 \
--conf spark.executor.memory=2g \
--conf spark.executor.cores=2 \
--class com.github.tashoyan.telecom.predictor.SparkPredictorMain \
"$jar_file" \
--kafka-brokers "$kafka_brokers" \
--kafka-event-topic "$kafka_event_topic" \
--kafka-alarm-topic "$kafka_alarm_topic" \
--checkpoint-dir "$checkpoint_dir" \
--watermark-interval-millis "$watermark_interval_millis" \
--problem-timeout-millis "$problem_timeout_millis"
