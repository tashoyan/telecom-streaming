# Telecom use cases implemented with Spark Streaming

[![Build Status](https://travis-ci.com/tashoyan/telecom-streaming.svg?branch=master)](https://travis-ci.com/tashoyan/telecom-streaming)

## Building the code

Run the command:
```bash
mvn clean install
```
Note that event samples are generated as a part of the build.

## Runtime dependencies

The following software is needed to run the applications provided in the `bin` directory.

The local machine:
* Spark 2.4.0 built with Scala 2.12
* Spark executables (for example,`spark-submit`) in the PATH
* Flink 1.7.2 built with Scala 2.12
* Flink executables (for example,`flink`) in the PATH
* Hadoop client distribution version 2.7.x
* Hadoop executables (for example, `hadoop` or `hdfs`) in the PATH
* The `HADOOP_CONF_DIR` environment variable points to the directory with Hadoop configuration files.

Remote dependencies:
* Hadoop cluster with HDFS and YARN
* Kafka cluster 2.1.0

Some references:
* [Running Spark on YARN](http://spark.apache.org/docs/latest/running-on-yarn.html)
* [Using Spark's "Hadoop Free" Build](https://spark.apache.org/docs/latest/hadoop-provided.html) - this is relevant for Spark 2.4.0 built with Scala 2.12
* [Flink YARN Setup](https://ci.apache.org/projects/flink/flink-docs-release-1.7/ops/deployment/yarn_setup.html)

## Preparing Kafka

1. Make sure that Zookeeper and Kafka are running.
1. Create the `events` and `alarms` Kafka topics:
   ```bash
   kafka-topics.sh --zookeeper localhost:2181 --create --topic events --partitions 5 --replication-factor 1
   kafka-topics.sh --zookeeper localhost:2181 --create --topic alarms --partitions 5 --replication-factor 1
   ```

## Running applications

All Spark and Flink applications are submitted to YARN.

You can configure settings by editing the shell launcher of the corresponding application in the `bin` directory.

### Event Generator

Event Generator pushes events to the `events` Kafka topic.
It takes the events from Parquet files supplied to the input HDFS directory.
Before sending events to Kafka,
Event Generator substitutes real event timestamps instead of dummy timestamps provided in the Parquet samples.

Run Event Generator:
```bash
./bin/event-generator.sh
```

### Event Writer

Event Writer consumes events from the `events` Kafka topic
and writes them to the output HDFS directory as Parquet files.
Event Writer partitions Parquet files by `siteId` and `year_month`.
The content of the output directory may be helpful in troubleshooting,
because it has the data actually sent by Event Generator.

Run Event Writer:
```bash
./bin/event-writer.sh
```

### Event Correlator

Event Correlator consumes events from the `events` Kafka topic, and correlates them.
It creates alarms as a result of the correlation, and sends alarms to the `alarms` Kafka topic. 

Run Event Correlator:
```bash
./bin/event-correlator.sh
```

### Spark Predictor

Spark Predictor consumes events from the `events` Kafka topic, and predicts failures based on the observed events.
It creates alarms as a result of the prediction, and sends alarms to the `alarms` Kafka topic. 

Run Spark Predictor:
```bash
./bin/spark-predictor.sh
```

## Pushing events and getting alarms

1. Run Kafka console consumer to get alarms on your terminal:
   ```bash
   kafka-console-consumer.sh --bootstrap-server localhost:9092 --group alarms --topic alarms
   ```
1. Put some samples with communication events to the input HDFS directory of Event Generator:
   ```bash
   hdfs dfs -put sampler/target/communication_events-controllers_2715_2716_all-1min-uniq.parquet /stream/input/events1.parquet
   hdfs dfs -put sampler/target/communication_events-controllers_2715_2716_all-1min-uniq.parquet /stream/input/events2.parquet
   hdfs dfs -put sampler/target/communication_events-controllers_2715_2716_all-1min-uniq.parquet /stream/input/events3.parquet
   ...
   ```
   Note that Parquet files in the input HDFS directory should have distinct names,
   to let Spark Streaming distinguish them as separate files.
   You can find more event samples in the `sampler/target/` directory.
   If you put event samples frequently enough,
   you will see `communication lost` alarms coming from the `alarms` topic to the Kafka console consumer:
   ```text
   {"timestamp":"2019-02-27T09:55:00.000+01:00","objectId":2716,"severity":"CRITICAL","info":"Communication lost with the controller"}
   {"timestamp":"2019-02-27T09:55:00.000+01:00","objectId":2715,"severity":"CRITICAL","info":"Communication lost with the controller"}
   {"timestamp":"2019-02-27T09:55:00.000+01:00","objectId":2715,"severity":"CRITICAL","info":"Communication lost with the controller"}
   {"timestamp":"2019-02-27T09:55:00.000+01:00","objectId":2716,"severity":"CRITICAL","info":"Communication lost with the controller"}
   ```
   Event Correlator produces these alarms as a result of topology based event correlation.
1. Put some samples with heat and smoke events to the input HDFS directory of Event Generator:
   ```bash
   hdfs dfs -put sampler/target/heat_events-site_1-1min-10_events.parquet /stream/input/events12.parquet
   hdfs dfs -put sampler/target/smoke_events-site_1-1min-10_events.parquet /stream/input/events13.parquet
   ```
   You will see `fire alarms` alarms coming from the `alarms` topic to the Kafka console consumer:
   ```text
   {"timestamp":"2019-02-27T11:52:40.000+01:00","objectId":1,"severity":"CRITICAL","info":"Fire on site 1"}
   ```
   Spark Predictor produces these alarms as a result of prediction based on heat and smoke events.

## Killing Spark applications

Use the `kill-yarn-apps.sh` tool and provide YARN identifiers for the applications you want to kill:
```bash
./bin/kill-yarn-apps.sh application_1544154479411_0830 application_1544154479411_0831 application_1544154479411_0837
```
The tool connects to YARN using your Hadoop settings exposed via the `HADOOP_CONF_DIR` environment variable.
