package com.github.tashoyan.telecom.predictor

import com.github.tashoyan.telecom.event.Event
import org.apache.spark.sql.streaming.GroupState

trait AlarmStateFunction {

  def updateAlarmState(siteId: Long, siteEvents: Iterator[Event], state: GroupState[ProblemState]): Iterator[Alarm]

}
