package com.github.tashoyan.telecom.generator

import java.sql.Timestamp
import java.util.concurrent.TimeUnit

import com.github.tashoyan.telecom.event.Event._
import com.github.tashoyan.telecom.spark.DataFrames.RichDataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StringType

object EventGeneratorMain extends EventGeneratorArgParser {
  private val spark = SparkSession.builder()
    .getOrCreate()
  spark.sparkContext
    .setLogLevel("WARN")

  def main(args: Array[String]): Unit = {
    parser.parse(args, EventGeneratorConfig()) match {
      case Some(config) => doMain(config)
      case None => sys.exit(1)
    }
  }

  //scalastyle:off
  private def doMain(config: EventGeneratorConfig): Unit = {
    println(config)
    val schema = spark.read
      .parquet(config.schemaFile)
      .schema

    val inputEvents = spark.readStream
      .schema(schema)
      .parquet(config.inputDir)

    val eventTimestampUdf = udf { (currentTimeSec: Long, dummyTimestamp: Timestamp) =>
      val timestampMillis: Long = TimeUnit.SECONDS.toMillis(currentTimeSec) - dummyTimestamp.getTime
      new Timestamp(timestampMillis)
    }
    val currentTimeSecColumn = "current_time_sec"
    val events = inputEvents
      .withColumn(currentTimeSecColumn, unix_timestamp())
      .withColumn(timestampColumn, eventTimestampUdf(col(currentTimeSecColumn), col(timestampColumn)))
      .drop(currentTimeSecColumn)

    val kafkaEvents = events
      .withJsonColumn(valueColumn)
      /*
      Kafka producer partition assignment: records having the same key go to the same topic partition.
      We ensure that events from the same site go through the same partition and preserve their order.
      Note that each Spark executor may send records to each Kafka broker hosting a partition.
      */
      //TODO Exactly once delivery: Kafka transactions and https://issues.apache.org/jira/browse/SPARK-25005
      .withColumn(keyColumn, col(siteIdColumn) cast StringType)

    val query = kafkaEvents
      .writeStream
      .outputMode(OutputMode.Append())
      .queryName(getClass.getSimpleName)
      .format("kafka")
      .option("kafka.bootstrap.servers", config.kafkaBrokers)
      .option("topic", config.kafkaTopic)
      .option("checkpointLocation", config.checkpointDir)
      .start()
    query.awaitTermination()
  }
  //scalastyle:on

}
