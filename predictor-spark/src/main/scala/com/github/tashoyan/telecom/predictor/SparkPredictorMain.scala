package com.github.tashoyan.telecom.predictor

import java.util.concurrent.TimeUnit

import com.github.tashoyan.telecom.event.{Alarm, DefaultEventDeduplicator, Event, KafkaEventReceiver}
import com.github.tashoyan.telecom.spark.DataFrames.RichDataset
import com.github.tashoyan.telecom.spark.KafkaStream.{keyColumn, valueColumn}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode}
import org.apache.spark.sql.types.StringType

object SparkPredictorMain extends SparkPredictorArgParser {

  def main(args: Array[String]): Unit = {
    parser.parse(args, SparkPredictorConfig()) match {
      case Some(config) => doMain(config)
      case None => sys.exit(1)
    }
  }

  private def doMain(config: SparkPredictorConfig): Unit = {
    println(config)

    implicit val spark: SparkSession = SparkSession.builder()
      .getOrCreate()
    spark.sparkContext
      .setLogLevel("WARN")
    import spark.implicits._

    val eventReceiver = new KafkaEventReceiver(config.kafkaBrokers, config.kafkaEventTopic)
    val eventDeduplicator = new DefaultEventDeduplicator(config.watermarkIntervalSec)
    val kafkaEvents = eventReceiver.receiveEvents()
    val events = eventDeduplicator.deduplicateEvents(kafkaEvents)

    val alarmStateFunction = new FireAlarmStateFunction(TimeUnit.SECONDS.toMillis(config.alarmTriggerIntervalSec.toLong))
    val alarms = events
      //TODO Maybe remove - deduplicator already set this watermark
      .withWatermark(Event.timestampColumn, s"${config.watermarkIntervalSec} seconds")
      .groupByKey(_.siteId)
      .flatMapGroupsWithState(OutputMode.Update(), GroupStateTimeout.EventTimeTimeout())(alarmStateFunction.updateAlarmState)

    val kafkaAlarms = alarms
      .withJsonColumn(valueColumn)
      .withColumn(keyColumn, col(Alarm.objectIdColumn) cast StringType)

    val query = kafkaAlarms
      .writeStream
      .outputMode(OutputMode.Update())
      .queryName(this.getClass.getSimpleName)
      .format("kafka")
      .option("kafka.bootstrap.servers", config.kafkaBrokers)
      .option("topic", config.kafkaAlarmTopic)
      .option("checkpointLocation", config.checkpointDir)
      .start()
    query.awaitTermination()
  }

}
