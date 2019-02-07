package com.github.tashoyan.telecom.event

import com.github.tashoyan.telecom.event.KafkaEventStream._
import com.github.tashoyan.telecom.spark.DataFrames.RichDataset
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.types.StringType

/**
  * Sends events in JSON format to a Kafka stream.
  *
  * @param kafkaBrokers    Kafka brokers to connect to.
  * @param kafkaTopic      Kafka topic to consume from.
  * @param partitionColumn Partition events on this column in the data set when writing them to the Kafka topic.
  * @param checkpointDir   Checkpoint directory used by Spark Kafka source.
  */
class KafkaEventSender(
    kafkaBrokers: String,
    kafkaTopic: String,
    partitionColumn: String,
    checkpointDir: String
) extends EventSender {

  override def sendEvents(events: Dataset[Event]): StreamingQuery = {
    val kafkaEvents = events
      .withJsonColumn(valueColumn)
      /*
      Kafka producer partition assignment: records having the same key go to the same topic partition.
      We ensure that events from the same site go through the same partition and preserve their order.
      Note that each Spark executor may send records to each Kafka broker hosting a partition.
      */
      //TODO Exactly once delivery: Kafka transactions and https://issues.apache.org/jira/browse/SPARK-25005
      .withColumn(keyColumn, col(partitionColumn) cast StringType)

    val query = kafkaEvents
      .writeStream
      .outputMode(OutputMode.Append())
      .queryName(getClass.getSimpleName)
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers)
      .option("topic", kafkaTopic)
      .option("checkpointLocation", checkpointDir)
      .start()
    query
  }

}
