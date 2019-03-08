package com.github.tashoyan.telecom.predictor

import com.github.tashoyan.telecom.event.FireAlarmUtil._
import com.github.tashoyan.telecom.event.{Alarm, Event}
import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode}
import org.apache.spark.sql.{Dataset, SparkSession}

class GroupStateFirePredictor(
    override val problemTimeoutMillis: Long,
    val watermarkIntervalMillis: Long
)(implicit spark: SparkSession) extends SparkFirePredictor {
  import spark.implicits._
  private val alarmStateFunction = new FireAlarmStateFunction(problemTimeoutMillis)

  override def predictAlarms(events: Dataset[Event]): Dataset[Alarm] = {
    val alarms = events
      .filter(e => isFireCandidate(e))
      .withWatermark(Event.timestampColumn, s"$watermarkIntervalMillis milliseconds")
      .groupByKey(_.siteId)
      .flatMapGroupsWithState(OutputMode.Update(), GroupStateTimeout.EventTimeTimeout())(alarmStateFunction.updateAlarmState)
    alarms
  }

}
