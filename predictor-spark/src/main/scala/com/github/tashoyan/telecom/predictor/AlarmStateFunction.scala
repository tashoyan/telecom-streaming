package com.github.tashoyan.telecom.predictor

import com.github.tashoyan.telecom.event.Alarm
import com.github.tashoyan.telecom.spark.SparkEvent
import org.apache.spark.sql.streaming.GroupState

//TODO This trait seems unneeded
trait AlarmStateFunction extends Serializable {

  def updateAlarmState(siteId: Long, siteEvents: Iterator[SparkEvent], state: GroupState[SparkFireAlarmState]): Iterator[Alarm]

}
