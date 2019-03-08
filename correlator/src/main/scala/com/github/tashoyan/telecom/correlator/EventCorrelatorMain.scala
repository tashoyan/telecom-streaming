package com.github.tashoyan.telecom.correlator

import com.github.tashoyan.telecom.event._
import com.github.tashoyan.telecom.spark.SparkEvent._
import com.github.tashoyan.telecom.spark.{DefaultEventDeduplicator, KafkaEventReceiver, KafkaStreamingSender, SparkEvent}
import com.github.tashoyan.telecom.topology.Topology._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
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

    val eventReceiver = new KafkaEventReceiver(config.kafkaBrokers, config.kafkaEventTopic)
    val eventDeduplicator = new DefaultEventDeduplicator(config.watermarkIntervalMillis)
    val kafkaEvents = eventReceiver.receiveEvents()
      .filter(_.isCommunication)
    val events = eventDeduplicator.deduplicateEvents(kafkaEvents)

    val controllerAlarms = correlateEvents(events, config)

    val alarmSender = new KafkaStreamingSender[Alarm](
      config.kafkaBrokers,
      config.kafkaAlarmTopic,
      Alarm.objectIdColumn,
      config.checkpointDir,
      OutputMode.Update()
    )
    val query = alarmSender.sendingQuery(controllerAlarms)
    query.awaitTermination()
  }

  private def correlateEvents(events: Dataset[SparkEvent], config: EventCorrelatorConfig)(implicit spark: SparkSession): Dataset[Alarm] = {
    import spark.implicits._

    val topology = spark.read
      .parquet(config.topologyFile)
    val totalStationCounts = topology
      .groupBy(controllerColumn)
      .agg(count(stationColumn) as "total_station_count")

    val affectedStationCounts = events
      /* Inner join - dropping events with unknown stations. Possible optimization: broadcast join. */
      .join(topology, col(siteIdColumn) === col(stationColumn), "inner")
      .groupBy(
        window(col(timestampColumn), s"${config.windowSizeMillis} milliseconds", s"${config.windowSlideMillis} milliseconds"),
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
    controllerAlarms
  }

}
