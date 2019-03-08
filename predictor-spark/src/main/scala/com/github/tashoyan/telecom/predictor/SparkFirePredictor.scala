package com.github.tashoyan.telecom.predictor

import com.github.tashoyan.telecom.event.{Alarm, Event}
import org.apache.spark.sql.Dataset

//TODO Spark-specific, should use SparkEvent instead of Event
trait SparkFirePredictor {

  val problemTimeoutMillis: Long

  def predictAlarms(events: Dataset[Event]): Dataset[Alarm]

}
