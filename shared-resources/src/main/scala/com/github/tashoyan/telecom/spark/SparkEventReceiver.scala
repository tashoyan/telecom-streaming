package com.github.tashoyan.telecom.spark

import org.apache.spark.sql.Dataset

/**
  * Streaming event receiver.
  */
trait SparkEventReceiver {

  /**
    * Receives a data set of events from a stream.
    * Implementation may produce side effects.
    *
    * @return Streaming data set of events.
    */
  def receiveEvents(): Dataset[SparkEvent]

}
