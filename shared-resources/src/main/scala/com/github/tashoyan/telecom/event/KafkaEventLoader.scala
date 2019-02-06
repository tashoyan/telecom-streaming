package com.github.tashoyan.telecom.event

import com.github.tashoyan.telecom.event.KafkaEventStream._
import com.github.tashoyan.telecom.spark.DataFrames.RichDataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Loads events in JSON format from a Kafka stream.
  *
  * @param kafkaBrokers  Kafka brokers to connect to.
  * @param kafkaTopic    Kafka topic to consume from.
  * @param pollTimeoutMs The timeout in milliseconds to poll data from Kafka topic.
  */
class KafkaEventLoader(
    kafkaBrokers: String,
    kafkaTopic: String,
    pollTimeoutMs: Long = defaultPollTimeoutMs
)(implicit spark: SparkSession) extends EventLoader {

  import spark.implicits._

  private val eventSchema = spark.emptyDataset[Event].schema

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

}
