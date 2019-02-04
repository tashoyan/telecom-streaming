package com.github.tashoyan.telecom.event

import org.apache.spark.sql.Dataset

/**
  * Event loader.
  * Loads events from a batch or a streaming data set.
  */
trait EventLoader {

  /**
    * Loads a data set of events.
    * Implementation may produce side effects.
    *
    * @return Data set of events. Implementation maybe batch or streaming.
    */
  def loadEvents(): Dataset[Event]

}
