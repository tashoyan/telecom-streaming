# Telecom use cases implemented with Spark Streaming

## Building the code

Run the command:
```bash
mvn clean install
```
Note that event samples are generated as a part of the build.

## Preparing Kafka

1. Make sure that Zookeeper and Kafka are running.
1. Create the `events` and `alarms` Kafka topics:
   ```bash
   kafka-topics.sh --zookeeper localhost:2181 --create --topic events --partitions 5 --replication-factor 1
   kafka-topics.sh --zookeeper localhost:2181 --create --topic alarms --partitions 5 --replication-factor 1
   ```

## Running Spark applications

You need the following environment:
- `spark-submit` command available in your `PATH`
- `HADOOP_CONF_DIR` environment variable points to the directory with Hadoop configuration files.

All Spark applications are submitted to YARN.

You can configure settings by editing the shell launcher of an application.

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

## Pushing events and getting alarms

1. Run Kafka console consumer to get alarms on your terminal:
   ```bash
   kafka-console-consumer.sh --bootstrap-server localhost:9092 --group alarms --topic alarms
   ```
1. Put some event samples to the input HDFS directory of Event Generator:
   ```bash
   hdfs dfs -put sampler/target/events_controllers_2715_2716_all_1min_uniq.parquet /stream/input/events1.parquet
   hdfs dfs -put sampler/target/events_controllers_2715_2716_all_1min_uniq.parquet /stream/input/events2.parquet
   hdfs dfs -put sampler/target/events_controllers_2715_2716_all_1min_uniq.parquet /stream/input/events3.parquet
   ...
   ```
   Note that Parquet files in the input HDFS directory should have distinct names,
   to let Spark Streaming distinguish them as separate files.
   You can find more event samples in the `sampler/target/` directory.
1. If you put event samples frequently enough,
   you will see alarms coming from the `alarms` topic to the Kafka console consumer:
   ```text
   {"controller":2716,"window":{"start":"2018-12-11T12:05:00.000+01:00","end":"2018-12-11T12:06:00.000+01:00"},"affected_station_count":116,"total_station_count":116}
   {"controller":2715,"window":{"start":"2018-12-11T12:05:00.000+01:00","end":"2018-12-11T12:06:00.000+01:00"},"affected_station_count":126,"total_station_count":126}
   {"controller":2716,"window":{"start":"2018-12-11T12:05:00.000+01:00","end":"2018-12-11T12:06:00.000+01:00"},"affected_station_count":116,"total_station_count":116}
   {"controller":2715,"window":{"start":"2018-12-11T12:05:00.000+01:00","end":"2018-12-11T12:06:00.000+01:00"},"affected_station_count":126,"total_station_count":126}
   ```

## Killing Spark applications

Use the `kill-yarn-apps.sh` tool and provide YARN identifiers for the applications you want to kill:
```bash
./bin/kill-yarn-apps.sh application_1544154479411_0830 application_1544154479411_0831 application_1544154479411_0837
```
The tool connects to YARN using your Hadoop settings exposed via the `HADOOP_CONF_DIR` environment variable.
