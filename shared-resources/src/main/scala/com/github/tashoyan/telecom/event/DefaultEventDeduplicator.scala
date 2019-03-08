package com.github.tashoyan.telecom.event

import com.github.tashoyan.telecom.event.Event._
import org.apache.spark.sql.Dataset

class DefaultEventDeduplicator(watermarkIntervalMillis: Long) extends EventDeduplicator {

  def deduplicateEvents(events: Dataset[Event]): Dataset[Event] = {
    events
      /*
        We have a case when a station does not provide unique identifiers for events.
        An event is identified by a pair (timestamp, siteId)
        */
      .withWatermark(timestampColumn, s"$watermarkIntervalMillis milliseconds")
      .dropDuplicates(timestampColumn, siteIdColumn)
  }

}
