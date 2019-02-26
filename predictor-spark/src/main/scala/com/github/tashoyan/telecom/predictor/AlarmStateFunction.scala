package com.github.tashoyan.telecom.predictor

import com.github.tashoyan.telecom.event.{Alarm, Event, ProblemState}
import org.apache.spark.sql.streaming.GroupState

trait AlarmStateFunction extends Serializable {

  def updateAlarmState(siteId: Long, siteEvents: Iterator[Event], state: GroupState[ProblemState]): Iterator[Alarm]

}
