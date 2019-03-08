package com.github.tashoyan.telecom.spark

import java.sql.Timestamp

import com.github.tashoyan.telecom.event.Event
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

case class SparkEvent(
    timestamp: Timestamp,
    siteId: Long,
    severity: String,
    info: String
) {

  def toEvent: Event =
    Event(
      timestamp.getTime,
      siteId,
      severity,
      info
    )
}

object SparkEvent {
  val timestampColumn = "timestamp"
  val siteIdColumn = "siteId"
  val severityColumn = "severity"
  val infoColumn = "info"

  val columns: Seq[String] = Seq(
    timestampColumn,
    siteIdColumn,
    severityColumn,
    infoColumn
  )

  def apply(event: Event): SparkEvent =
    SparkEvent(
      new Timestamp(event.timestamp),
      event.siteId,
      event.severity,
      event.info
    )

  private def dataFrameAsEventDataset(df: DataFrame)(implicit spark: SparkSession): Dataset[SparkEvent] = {
    import spark.implicits._
    df.select(Event.columns.head, Event.columns.tail: _*)
      .as[Event]
      .map(SparkEvent(_))
  }

  implicit class SparkEventDataFrame(val df: DataFrame) extends AnyVal {

    @inline def asSparkEvents(implicit spark: SparkSession): Dataset[SparkEvent] = {
      dataFrameAsEventDataset(df)
    }

  }

}
