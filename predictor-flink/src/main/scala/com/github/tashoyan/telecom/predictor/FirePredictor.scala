package com.github.tashoyan.telecom.predictor

import com.github.tashoyan.telecom.event.{Alarm, Event}
import org.apache.flink.streaming.api.scala.DataStream

trait FirePredictor {

  val problemTimeoutMillis: Long

  def predictAlarms(events: DataStream[Event]): DataStream[Alarm]

}

object FirePredictor {

  def isFireCandidate(event: Event): Boolean =
    event.isHeat || event.isSmoke

  val alarmSeverity: String = "CRITICAL"

}
