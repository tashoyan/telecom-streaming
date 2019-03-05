package com.github.tashoyan.telecom.predictor

import com.github.tashoyan.telecom.event.{Alarm, Event}
import org.apache.flink.streaming.api.scala.DataStream

trait FirePredictor {

  def predictAlarms(events: DataStream[Event]): DataStream[Alarm]

}
