package com.github.tashoyan.telecom.correlator

import com.github.tashoyan.telecom.util.DataFrames.RichDataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}

object SitePowerFailureMain extends SitePowerFailureArgParser {

  def main(args: Array[String]): Unit = {
    parser.parse(args, SitePowerFailureConfig()) match {
      case Some(config) => doMain(config)
      case None => sys.exit(1)
    }
  }

  private def doMain(config: SitePowerFailureConfig): Unit = {
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
      .option("subscribe", config.eventKafkaTopic)
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", "false")
      .load()

    val jsonColumn = "json_value"
    val events = kafkaEvents
      .select(col("value") cast StringType as jsonColumn)
      .parseJsonColumn(jsonColumn, schema)
      .drop(jsonColumn)

    //TODO Dummy power failure filter - works with SFR sample
    val isPowerFailure = lower(col("probablecausename")) === "PowerProblem".toLowerCase
    val powerFailures = events.where(isPowerFailure)

    val writer = new ForeachWriter[Row] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Row): Unit = {
        val probableCause = value.getAs[String]("probablecausename")
        val id = value.getAs[Long]("identifier")
        val oc = value.getAs[String]("operationcontextname")
        println(s"$oc,$id: $probableCause")
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }
    val query = powerFailures
      .writeStream
      .outputMode(OutputMode.Update())
      .foreach(writer)
      .option("checkpointLocation", config.checkpointDir)
      .start()
    query.awaitTermination()

  }

}
