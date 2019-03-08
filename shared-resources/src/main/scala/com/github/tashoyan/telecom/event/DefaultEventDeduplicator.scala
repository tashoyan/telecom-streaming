package com.github.tashoyan.telecom.event

import com.github.tashoyan.telecom.event.SparkEvent._
import org.apache.spark.sql.Dataset

class DefaultEventDeduplicator(watermarkIntervalMillis: Long) extends EventDeduplicator {

  def deduplicateEvents(events: Dataset[SparkEvent]): Dataset[SparkEvent] = {
    events
      /*
        We have a case when a station does not provide unique identifiers for events.
        An event is identified by a pair (timestamp, siteId)
        */
      .withWatermark(timestampColumn, s"$watermarkIntervalMillis milliseconds")
      .dropDuplicates(timestampColumn, siteIdColumn)
  }

}
