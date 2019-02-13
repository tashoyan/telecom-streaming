package com.github.tashoyan.telecom.event

import com.github.tashoyan.telecom.event.Event._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SparkEventAdapter {

  private def dataFrameAsEventDataset(df: DataFrame)(implicit spark: SparkSession): Dataset[Event] = {
    import spark.implicits._
    df.select(columns.head, columns.tail: _*)
      .as[Event]
  }

  implicit class EventDataFrame(val df: DataFrame) extends AnyVal {

    @inline def asEvents(implicit spark: SparkSession): Dataset[Event] = {
      dataFrameAsEventDataset(df)
    }
  }

}
