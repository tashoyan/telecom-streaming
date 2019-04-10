package com.github.tashoyan.telecom.predictor

import com.github.tashoyan.telecom.event.Alarm
import com.github.tashoyan.telecom.event.FireAlarmUtil._
import com.github.tashoyan.telecom.spark.SparkEvent
import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode}
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Fire Predictor implementation based on
  * Spark [[https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.streaming.GroupState GroupState API]].
  *
  * @param problemTimeoutMillis    Problem timeout in milliseconds.
  *                                If the interval between a heat event and a smoke event exceeds this timeout,
  *                                these two events are considered as uncorrelated.
  * @param watermarkIntervalMillis Watermark interval in milliseconds.
  *                                Used by Spark for grouping aggregation on the stream of events.
  * @param spark                   Spark session.
  */
class GroupStateFirePredictor(
    override val problemTimeoutMillis: Long,
    val watermarkIntervalMillis: Long
)(implicit spark: SparkSession) extends SparkFirePredictor {
  import spark.implicits._

  override def predictAlarms(events: Dataset[SparkEvent]): Dataset[Alarm] = {
    val alarmStateFunction = new FireAlarmStateFunction(problemTimeoutMillis)
    val alarms = events
      .filter(e => isFireCandidate(e))
      .withWatermark(SparkEvent.timestampColumn, s"$watermarkIntervalMillis milliseconds")
      .groupByKey(_.siteId)
      .flatMapGroupsWithState(OutputMode.Update(), GroupStateTimeout.EventTimeTimeout())(alarmStateFunction.updateAlarmState)
    alarms
  }

}
