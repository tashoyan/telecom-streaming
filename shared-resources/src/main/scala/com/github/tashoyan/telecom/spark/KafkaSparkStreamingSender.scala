package com.github.tashoyan.telecom.spark

import com.github.tashoyan.telecom.spark.DataFrames.RichDataset
import com.github.tashoyan.telecom.spark.SparkKafkaStream.{keyColumn, valueColumn}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.types.StringType

/**
  * Sends data in JSON format to a Kafka stream.
  *
  * @param kafkaBrokers    Kafka brokers to connect to.
  * @param kafkaTopic      Kafka topic to consume from.
  * @param partitionColumn Partition the data on this column in the data set when writing to the Kafka topic.
  * @param checkpointDir   Checkpoint directory used by Spark Kafka source.
  * @param outputMode      Output mode of the streaming query.
  */
class KafkaSparkStreamingSender[T <: Product](
    kafkaBrokers: String,
    kafkaTopic: String,
    partitionColumn: String,
    checkpointDir: String,
    outputMode: OutputMode
) extends SparkStreamingSender[T] {

  override def sendingQuery(data: Dataset[T]): StreamingQuery = {
    val jsonData = data
      .withJsonColumn(valueColumn)
      /*
      Kafka producer partition assignment: records having the same key go to the same topic partition.
      We ensure that events from the same site go through the same partition and preserve their order.
      Note that each Spark executor may send records to each Kafka broker hosting a partition.
      */
      //TODO Exactly once delivery: Kafka transactions and https://issues.apache.org/jira/browse/SPARK-25005
      .withColumn(keyColumn, col(partitionColumn) cast StringType)

    val query = jsonData
      .writeStream
      .outputMode(outputMode)
      .queryName(getClass.getSimpleName)
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers)
      .option("topic", kafkaTopic)
      .option("checkpointLocation", checkpointDir)
      .start()
    query
  }

}
