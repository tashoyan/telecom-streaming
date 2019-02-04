package com.github.tashoyan.telecom.event

import org.apache.spark.sql.Dataset

trait EventDeduplicator {

  def deduplicateEvents(events: Dataset[Event]): Dataset[Event]

}
