package com.github.tashoyan.telecom.event

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.streaming.StreamingQuery

/**
  * IO functions for event streams
  */
trait EventStream {

  /**
    * Loads a data set of events from a stream.
    * Implementation may produce side effects.
    *
    * @return Data set of events.
    */
  def loadEvents(): Dataset[Event]

  /**
    * Creates a query to write events to a stream.
    *
    * @param events Data set of events.
    * @return Handler to the streaming query.
    */
  def writeEvents(events: Dataset[Event]): StreamingQuery

}
