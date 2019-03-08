package com.github.tashoyan.telecom.spark

import org.apache.spark.sql.Dataset

trait EventDeduplicator {

  def deduplicateEvents(events: Dataset[SparkEvent]): Dataset[SparkEvent]

}
