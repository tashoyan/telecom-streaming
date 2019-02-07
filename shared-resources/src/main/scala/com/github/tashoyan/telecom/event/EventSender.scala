package com.github.tashoyan.telecom.event

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.streaming.StreamingQuery

/**
  * Streaming event sender.
  */
trait EventSender {

  /**
    * Creates a streaming query to send events to a stream.
    *
    * @param events Streaming data set of events.
    * @return Handler to the streaming query.
    */
  def sendEvents(events: Dataset[Event]): StreamingQuery

}
