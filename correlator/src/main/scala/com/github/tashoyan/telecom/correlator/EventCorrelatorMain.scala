package com.github.tashoyan.telecom.correlator

import com.github.tashoyan.telecom.event.Event._
import com.github.tashoyan.telecom.spark.DataFrames.RichDataFrame
import com.github.tashoyan.telecom.topology.Topology._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StringType

object EventCorrelatorMain extends EventCorrelatorArgParser {
  private val spark = SparkSession.builder()
    .getOrCreate()
  spark.sparkContext
    .setLogLevel("WARN")

  def main(args: Array[String]): Unit = {
    parser.parse(args, EventCorrelatorConfig()) match {
      case Some(config) => doMain(config)
      case None => sys.exit(1)
    }
  }

  //TODO Refactor and enable scalastyle
  //scalastyle:off
  private def doMain(config: EventCorrelatorConfig): Unit = {
    println(config)
    val schema = spark.read
      .parquet(config.schemaFile)
      .schema

    val topology = spark.read
      .parquet(config.topologyFile)
    val totalStationCounts = topology
      .groupBy(controllerColumn)
      .agg(count(stationColumn) as "total_station_count")

    val kafkaEvents = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config.kafkaBrokers)
      .option("subscribe", config.kafkaInputTopic)
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", "false")
      .load()

    val jsonColumn = "json_value"
    val events = kafkaEvents
      .select(col("value") cast StringType as jsonColumn)
      .parseJsonColumn(jsonColumn, schema)
      .drop(jsonColumn)
      .withWatermark(timestampColumn, s"${config.watermarkIntervalSec} seconds")
      .dropDuplicates(timestampColumn, siteIdColumn)

    val affectedStationCounts = events
      /*
      Inner join - dropping events with unknown stations.
      Possible optimization: broadcast join.
      */
      .join(topology, col(siteIdColumn) === col(stationColumn), "inner")
      .groupBy(
        window(col(timestampColumn), s"${config.windowSizeSec} seconds", s"${config.windowShiftSec} seconds"),
        col(controllerColumn)
      )
      /*
      Workaround: countDistinct() is unsupported for streaming data sets (Spark 2.4.0)
      */
      .agg(collect_set(siteIdColumn) as "affected_stations")
      .withColumn("affected_station_count", size(col("affected_stations")))
      .drop("affected_stations")

    val controllerAlarms = affectedStationCounts
      .join(totalStationCounts, Seq(controllerColumn), "inner")
      .where(col("affected_station_count") === col("total_station_count"))

    val kafkaAlarms = controllerAlarms
      .withJsonColumn(valueColumn)
      .withColumn(keyColumn, col(controllerColumn) cast StringType)

    val query = kafkaAlarms
      .writeStream
      .outputMode(OutputMode.Update())
      .queryName(getClass.getSimpleName)
      .format("kafka")
      .option("kafka.bootstrap.servers", config.kafkaBrokers)
      .option("topic", config.kafkaOutputTopic)
      .option("checkpointLocation", config.checkpointDir)
      .start()
    query.awaitTermination()
  }
  //scalastyle:on

}
