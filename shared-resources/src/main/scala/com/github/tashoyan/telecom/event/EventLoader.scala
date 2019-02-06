package com.github.tashoyan.telecom.event

import org.apache.spark.sql.Dataset

/**
  * Streaming event loader.
  */
trait EventLoader {

  /**
    * Loads a data set of events from a stream.
    * Implementation may produce side effects.
    *
    * @return Streaming data set of events.
    */
  def loadEvents(): Dataset[Event]

}
