package com.github.tashoyan.telecom.spark

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.streaming.StreamingQuery

/**
  * Streaming sender.
  */
trait StreamingSender[T <: Product] {

  /**
    * Creates a streaming query to send data to a stream.
    *
    * @param data Streaming data set to send.
    * @return Handler to the streaming query.
    */
  def sendingQuery(data: Dataset[T]): StreamingQuery

}
