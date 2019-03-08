package com.github.tashoyan.telecom.predictor

import com.github.tashoyan.telecom.event.{Alarm, Event}
import org.apache.flink.streaming.api.scala.DataStream

trait FlinkFirePredictor {

  val problemTimeoutMillis: Long

  def predictAlarms(events: DataStream[Event]): DataStream[Alarm]

}
