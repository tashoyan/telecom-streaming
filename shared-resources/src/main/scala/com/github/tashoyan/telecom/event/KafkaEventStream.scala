package com.github.tashoyan.telecom.event

import com.github.tashoyan.telecom.event.KafkaEventStream._
import com.github.tashoyan.telecom.spark.DataFrames.{RichDataFrame, RichDataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Functions to read/write events from/to a Kafka stream.
  *
  * @param kafkaBrokers    Kafka brokers to connect to.
  * @param kafkaTopic      Kafka topic to consume from.
  * @param partitionColumn Partition events on this column in the data set when writing them to the Kafka topic.
  * @param checkpointDir   Checkpoint directory used by Spark Kafka source.
  * @param pollTimeoutMs   The timeout in milliseconds to poll data from Kafka topic.
  * @param spark           Spark session
  */
//TODO validate args
class KafkaEventStream(
    kafkaBrokers: String,
    kafkaTopic: String,
    partitionColumn: String,
    checkpointDir: String,
    pollTimeoutMs: Long = defaultPollTimeoutMs
)(implicit spark: SparkSession) extends EventStream {

  import spark.implicits._

  private val eventSchema =
    spark.emptyDataset[Event].schema

  override def loadEvents(): Dataset[Event] = {
    /*
     TODO Why only one Kafka consumer?
     https://stackoverflow.com/questions/53605061/spark-structured-streaming-kafka-source-how-many-consumers
    */
    val kafkaEvents = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers)
      .option("subscribe", kafkaTopic)
      .option("kafkaConsumer.pollTimeoutMs", pollTimeoutMs)
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", "false")
      .load()

    val jsonColumn = "json_value"
    val events = kafkaEvents
      .select(col(valueColumn) cast StringType as jsonColumn)
      .parseJsonColumn(jsonColumn, eventSchema)
      .select(Event.columns.map(col).toSeq: _*)
      .as[Event]
    events
  }

  override def writeEvents(events: Dataset[Event]): StreamingQuery = {
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

object KafkaEventStream {

  /**
    * Data set column used for keys when reading/writing data from/to Kafka topics.
    * This value is specified in the Structured Streaming + Kafka Integration Guide.
    */
  val keyColumn = "key"

  /**
    * Data set column used for values when reading/writing data from/to Kafka topics.
    * This value is specified in the Structured Streaming + Kafka Integration Guide.
    */
  val valueColumn = "value"

  /**
    * Default value of the timeout to poll data from Kafka topics.
    * This value is specified in the Structured Streaming + Kafka Integration Guide.
    */
  val defaultPollTimeoutMs: Long = 512

}
