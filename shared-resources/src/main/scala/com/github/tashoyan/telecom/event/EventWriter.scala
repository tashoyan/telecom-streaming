package com.github.tashoyan.telecom.event

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.streaming.StreamingQuery

/**
  * Streaming event writer.
  */
trait EventWriter {

  /**
    * Creates a streaming query to write events to a stream.
    *
    * @param events Streaming data set of events.
    * @return Handler to the streaming query.
    */
  def writeEvents(events: Dataset[Event]): StreamingQuery

}
