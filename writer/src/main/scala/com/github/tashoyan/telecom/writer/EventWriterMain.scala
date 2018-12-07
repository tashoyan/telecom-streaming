package com.github.tashoyan.telecom.writer

import java.sql.Timestamp

import com.github.tashoyan.telecom.event.Event._
import com.github.tashoyan.telecom.spark.DataFrames.RichDataFrame
import com.github.tashoyan.telecom.util.Timestamps._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StringType

object EventWriterMain extends EventWriterArgParser {
  private val spark = SparkSession.builder()
    .getOrCreate()
  spark.sparkContext
    .setLogLevel("WARN")

  def main(args: Array[String]): Unit = {
    parser.parse(args, EventWriterConfig()) match {
      case Some(config) => doMain(config)
      case None => sys.exit(1)
    }
  }

  private def doMain(config: EventWriterConfig): Unit = {
    println(config)
    val schema = spark.read
      .parquet(config.schemaFile)
      .schema

    /*
    TODO Why only one Kafka consumer?
    https://stackoverflow.com/questions/53605061/spark-structured-streaming-kafka-source-how-many-consumers
     */
    val kafkaEvents = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config.kafkaBrokers)
      .option("subscribe", config.kafkaTopic)
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", "false")
      .load()

    val jsonColumn = "json_value"
    val yearMonthColumn = "year_month"
    val yearMonthUdf = udf { origEventTime: Timestamp => yearMonth(origEventTime) }
    val events = kafkaEvents
      .select(col("value") cast StringType as jsonColumn)
      .parseJsonColumn(jsonColumn, schema)
      .drop(jsonColumn)
      //TODO Configurable whatermark
      /*
      TODO Explain in the article: deduplicate events by site and timestamp
      We have a case when a station does not provide unique identifiers for events.
      */
      .withWatermark(timestampColumn, "10 minutes")
      .dropDuplicates(timestampColumn, siteIdColumn)
      //TODO Explain in the article: partition by stations, to evenly distribute the load on Spark executors
      .repartition(col(siteIdColumn))
      .withColumn(yearMonthColumn, yearMonthUdf(col(timestampColumn)))

    val query = events
      .writeStream
      .outputMode(OutputMode.Append())
      .queryName(getClass.getSimpleName)
      //TODO Explain in the article - partition Parquet storage
      .partitionBy(siteIdColumn, yearMonthColumn)
      .format("parquet")
      .option("path", config.outputDir)
      .option("checkpointLocation", config.checkpointDir)
      .start()
    query.awaitTermination()
  }

}
