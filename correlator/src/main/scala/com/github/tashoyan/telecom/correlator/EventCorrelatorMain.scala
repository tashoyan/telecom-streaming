package com.github.tashoyan.telecom.correlator

import com.github.tashoyan.telecom.event.Event._
import com.github.tashoyan.telecom.event.{Alarm, DefaultEventDeduplicator, KafkaEventReceiver}
import com.github.tashoyan.telecom.spark.DataFrames.RichDataset
import com.github.tashoyan.telecom.spark.KafkaStream._
import com.github.tashoyan.telecom.topology.Topology._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Dataset, SparkSession}

object EventCorrelatorMain extends EventCorrelatorArgParser {

  def main(args: Array[String]): Unit = {
    parser.parse(args, EventCorrelatorConfig()) match {
      case Some(config) => doMain(config)
      case None => sys.exit(1)
    }
  }

  private def doMain(config: EventCorrelatorConfig): Unit = {
    println(config)

    implicit val spark: SparkSession = SparkSession.builder()
      .getOrCreate()
    spark.sparkContext
      .setLogLevel("WARN")
    import spark.implicits._

    val topology = spark.read
      .parquet(config.topologyFile)
    val totalStationCounts = topology
      .groupBy(controllerColumn)
      .agg(count(stationColumn) as "total_station_count")

    val eventReceiver = new KafkaEventReceiver(config.kafkaBrokers, config.kafkaEventTopic)
    val eventDeduplicator = new DefaultEventDeduplicator(config.watermarkIntervalSec)
    val kafkaEvents = eventReceiver.receiveEvents()
      .filter(_.isCommunication)
    val events = eventDeduplicator.deduplicateEvents(kafkaEvents)

    val affectedStationCounts = events
      /* Inner join - dropping events with unknown stations. Possible optimization: broadcast join. */
      .join(topology, col(siteIdColumn) === col(stationColumn), "inner")
      .groupBy(
        window(col(timestampColumn), s"${config.windowSizeSec} seconds", s"${config.windowShiftSec} seconds"),
        col(controllerColumn)
      )
      /* Workaround: countDistinct() is unsupported for streaming data sets (Spark 2.4.0) */
      .agg(collect_set(siteIdColumn) as "affected_stations")
      .withColumn("affected_station_count", size(col("affected_stations")))
      .drop("affected_stations")

    val controllerAlarms = affectedStationCounts
      .join(totalStationCounts, Seq(controllerColumn), "inner")
      .where(col("affected_station_count") === col("total_station_count"))
      .select(
        col("window.start") as Alarm.timestampColumn,
        col(controllerColumn) as Alarm.objectIdColumn,
        lit("CRITICAL") as Alarm.severityColumn,
        lit("Communication lost with the controller") as Alarm.infoColumn
      )
      .as[Alarm]

    val query = createAndStartAlarmQuery(config, controllerAlarms)
    query.awaitTermination()
  }

  //TODO Makes sense to extract alarm stream to a separate class
  private def createAndStartAlarmQuery(config: EventCorrelatorConfig, alarms: Dataset[Alarm]): StreamingQuery = {
    val kafkaAlarms = alarms
      .withJsonColumn(valueColumn)
      .withColumn(keyColumn, col(Alarm.objectIdColumn) cast StringType)

    val query = kafkaAlarms
      .writeStream
      .outputMode(OutputMode.Update())
      .queryName(getClass.getSimpleName)
      .format("kafka")
      .option("kafka.bootstrap.servers", config.kafkaBrokers)
      .option("topic", config.kafkaAlarmTopic)
      .option("checkpointLocation", config.checkpointDir)
      .start()
    query
  }

}
