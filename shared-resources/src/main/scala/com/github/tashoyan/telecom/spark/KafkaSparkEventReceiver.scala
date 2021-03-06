package com.github.tashoyan.telecom.spark

import java.sql.Timestamp

import com.github.tashoyan.telecom.event.Event
import com.github.tashoyan.telecom.spark.DataFrames.RichDataFrame
import com.github.tashoyan.telecom.spark.SparkKafkaStream._
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
class KafkaSparkEventReceiver(
    kafkaBrokers: String,
    kafkaTopic: String
)(implicit spark: SparkSession) extends SparkEventReceiver {

  import spark.implicits._

  private val eventSchema = spark.emptyDataset[Event].schema

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
    val eventTimestampUdf = udf { timestampMillis: Long => new Timestamp(timestampMillis) }
    val sparkEvents: Dataset[SparkEvent] = kafkaEvents
      .select(col(valueColumn) cast StringType as jsonColumn)
      .parseJsonColumn(jsonColumn, eventSchema)
      .withColumn(SparkEvent.timestampColumn, eventTimestampUdf(col(Event.timestampColumn)))
      .asSparkEvents
    sparkEvents
  }

}
