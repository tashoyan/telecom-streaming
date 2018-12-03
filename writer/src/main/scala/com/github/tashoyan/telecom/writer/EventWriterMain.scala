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

  def main(args: Array[String]): Unit = {
    parser.parse(args, EventWriterConfig()) match {
      case Some(config) => doMain(config)
      case None => sys.exit(1)
    }
  }

  private def doMain(config: EventWriterConfig): Unit = {
    val spark = SparkSession.builder()
      .appName(getClass.getSimpleName)
      .getOrCreate()
    spark.sparkContext
      .setLogLevel("WARN")

    val schema = spark.read
      .parquet(config.schemaFile)
      .schema

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
      .withColumn(yearMonthColumn, yearMonthUdf(col(timestampColumn)))
      //TODO Is it really necessary - repartition Spark data frame?
      .repartition(col(yearMonthColumn))

    val query = events
      .writeStream
      .outputMode(OutputMode.Append())
      //TODO Explain in the article - partition Parquet storage
      .partitionBy(yearMonthColumn)
      .format("parquet")
      .option("path", config.outputDir)
      .option("checkpointLocation", config.checkpointDir)
      .start()
    query.awaitTermination()
  }

}
