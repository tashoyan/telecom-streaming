package com.github.tashoyan.telecom.predictor

import com.github.tashoyan.telecom.event.Alarm
import com.github.tashoyan.telecom.spark.SparkEvent
import org.apache.spark.sql.Dataset

/**
  * Trait to implement Fire Predictor with Spark Structured Streaming.
  */
trait SparkFirePredictor {

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
  def predictAlarms(events: Dataset[SparkEvent]): Dataset[Alarm]

}
