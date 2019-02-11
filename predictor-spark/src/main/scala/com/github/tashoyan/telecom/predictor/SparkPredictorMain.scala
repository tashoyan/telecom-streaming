package com.github.tashoyan.telecom.predictor

import java.sql.Timestamp

import com.github.tashoyan.telecom.event.{DefaultEventDeduplicator, Event, KafkaEventReceiver}
import com.github.tashoyan.telecom.spark.DataFrames.RichDataset
import com.github.tashoyan.telecom.spark.KafkaStream.{keyColumn, valueColumn}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.types.StringType

object SparkPredictorMain extends SparkPredictorArgParser {
  private val fireTriggerTimeout = 20000L

  def main(args: Array[String]): Unit = {
    parser.parse(args, SparkPredictorConfig()) match {
      case Some(config) => doMain(config)
      case None => sys.exit(1)
    }
  }

  //TODO Refactor and enale scalastyle back
  //scalastyle:off
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
    events.writeStream
      .outputMode(OutputMode.Append())
      .format("console")
      .option("truncate", value = false)
      .start()

    def alarmFunction: (Long, Iterator[Event], GroupState[ProblemState]) => Iterator[Alarm] = {
      (siteId, events, state) =>
        if (state.hasTimedOut) {
          println(s"TIMED OUT STATE; events: $events")
          state.remove()
          Iterator.empty
        } else if (state.exists) {
          println(s"EXISTING STATE; events: $events")
          val heatTimestamp = state.get.heatTimestamp
          val smokeEvent = events
            .filter(e => e.info != null && e.info.toLowerCase.contains("smoke"))
            .toStream
            .headOption
          //TODO Additional 'heat'
          //TODO More than one 'smoke'
          val smokeTimestamp = smokeEvent.map(_.timestamp)
          if (smokeTimestamp.isDefined) {
            println(s"smoke: $smokeTimestamp")
            println(s"heat: $heatTimestamp")
            if (smokeTimestamp.get.getTime - heatTimestamp.getTime > 0 && smokeTimestamp.get.getTime - heatTimestamp.getTime <= fireTriggerTimeout) {
              val alarm = Alarm(siteId, heatTimestamp, smokeTimestamp.get, s"Fire alarm on $siteId")
              state.remove()
              Iterator(alarm)
            } else {
              state.update(state.get)
              state.setTimeoutDuration(20000)
              Iterator.empty
            }
          } else {
            state.update(state.get)
            state.setTimeoutDuration(20000)
            Iterator.empty
          }
        } else {
          println(s"NO STATE; events: $events")
          val heatEvent = events
            .filter(e => e.info != null && e.info.toLowerCase.contains("heat"))
            .toStream
            .headOption
          println(s"heat event: $heatEvent")
          //TODO What if 'smoke' comes in the same batch as 'heat'?
          //TODO What if there are more than one 'heat'?
          val newProblemState = heatEvent.map(e => ProblemState(siteId, e.timestamp))
          if (newProblemState.isDefined) {
            state.update(newProblemState.get)
            state.setTimeoutDuration(20000)
          }
          Iterator.empty
        }
    }

    val alarms = events
      .groupByKey(_.siteId)
      //TODO EventTimeTimeout
      .flatMapGroupsWithState[ProblemState, Alarm](OutputMode.Update(), GroupStateTimeout.ProcessingTimeTimeout())(alarmFunction)

    //TODO Complete

    //TODO Alarm columns
    val kafkaAlarms = alarms
      .withJsonColumn(valueColumn)
      .withColumn(keyColumn, col("siteId") cast StringType)

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

case class ProblemState(siteId: Long, heatTimestamp: Timestamp)

case class Alarm(siteId: Long, heatTimestamp: Timestamp, smokeTimestamp: Timestamp, info: String)
