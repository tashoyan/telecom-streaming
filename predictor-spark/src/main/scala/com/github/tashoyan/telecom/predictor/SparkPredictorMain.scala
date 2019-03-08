package com.github.tashoyan.telecom.predictor

import com.github.tashoyan.telecom.event._
import com.github.tashoyan.telecom.spark.{DefaultEventDeduplicator, KafkaEventReceiver, KafkaStreamingSender}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{Dataset, SparkSession}

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
    val eventDeduplicator = new DefaultEventDeduplicator(config.watermarkIntervalMillis)
    val kafkaEvents = eventReceiver.receiveEvents()
    //TODO Filter fire candidates before deduplication
    val events: Dataset[Event] = eventDeduplicator
      .deduplicateEvents(kafkaEvents)
      .map(_.toEvent)

    val firePredictor = new GroupStateFirePredictor(config.problemTimeoutMillis, config.watermarkIntervalMillis)
    val alarms = firePredictor.predictAlarms(events)

    val alarmSender = new KafkaStreamingSender[Alarm](
      config.kafkaBrokers,
      config.kafkaAlarmTopic,
      Alarm.objectIdColumn,
      config.checkpointDir,
      OutputMode.Update()
    )
    val query = alarmSender.sendingQuery(alarms)
    query.awaitTermination()
  }

}
