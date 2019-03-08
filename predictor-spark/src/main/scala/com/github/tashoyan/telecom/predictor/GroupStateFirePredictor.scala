package com.github.tashoyan.telecom.predictor

import com.github.tashoyan.telecom.event.Alarm
import com.github.tashoyan.telecom.event.FireAlarmUtil._
import com.github.tashoyan.telecom.spark.SparkEvent
import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode}
import org.apache.spark.sql.{Dataset, SparkSession}

class GroupStateFirePredictor(
    override val problemTimeoutMillis: Long,
    val watermarkIntervalMillis: Long
)(implicit spark: SparkSession) extends SparkFirePredictor {
  import spark.implicits._
  private val alarmStateFunction = new FireAlarmStateFunction(problemTimeoutMillis)

  override def predictAlarms(events: Dataset[SparkEvent]): Dataset[Alarm] = {
    val alarms = events
      .filter(e => isFireCandidate(e))
      .withWatermark(SparkEvent.timestampColumn, s"$watermarkIntervalMillis milliseconds")
      .groupByKey(_.siteId)
      .flatMapGroupsWithState(OutputMode.Update(), GroupStateTimeout.EventTimeTimeout())(alarmStateFunction.updateAlarmState)
    alarms
  }

}
