package com.github.tashoyan.telecom.predictor

import com.github.tashoyan.telecom.event.{Alarm, Event}
import org.apache.flink.streaming.api.scala.DataStream

/**
  * Trait to implement Fire Predictor with Flink.
  */
trait FlinkFirePredictor {

  /**
    * Problem timeout in milliseconds.
    * If the interval between a heat event and a smoke event exceeds this timeout,
    * these two events are considered as uncorrelated.
    */
  val problemTimeoutMillis: Long

  /**
    * Make a prediction on fire alarm.
    *
    * @param events Stream of events, having heat and smoke events.
    * @return Stream of alarms.
    */
  def predictAlarms(events: DataStream[Event]): DataStream[Alarm]

}
