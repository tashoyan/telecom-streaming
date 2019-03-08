package com.github.tashoyan.telecom.predictor

import com.github.tashoyan.telecom.event.Alarm
import com.github.tashoyan.telecom.spark.SparkEvent
import org.apache.spark.sql.Dataset

trait SparkFirePredictor {

  val problemTimeoutMillis: Long

  def predictAlarms(events: Dataset[SparkEvent]): Dataset[Alarm]

}
