package com.github.tashoyan.telecom.spark

import com.github.tashoyan.telecom.spark.DataFrames.RichDataFrame
import com.github.tashoyan.telecom.spark.KafkaStream._
import com.github.tashoyan.telecom.spark.SparkEvent._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Receives events in JSON format from a Kafka stream.
  *
  * @param kafkaBrokers  Kafka brokers to connect to.
  * @param kafkaTopic    Kafka topic to consume from.
  */
class KafkaEventReceiver(
    kafkaBrokers: String,
    kafkaTopic: String
)(implicit spark: SparkSession) extends EventReceiver {

  import spark.implicits._

  private val eventSchema = spark.emptyDataset[SparkEvent].schema

  override def receiveEvents(): Dataset[SparkEvent] = {
    /*
    TODO Why only one Kafka consumer?
    https://stackoverflow.com/questions/53605061/spark-structured-streaming-kafka-source-how-many-consumers
     */
    val kafkaEvents = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers)
      .option("subscribe", kafkaTopic)
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", "false")
      .load()

    val jsonColumn = "json_value"
    val events: Dataset[SparkEvent] = kafkaEvents
      .select(col(valueColumn) cast StringType as jsonColumn)
      .parseJsonColumn(jsonColumn, eventSchema)
      .asSparkEvents
    events
  }

}
