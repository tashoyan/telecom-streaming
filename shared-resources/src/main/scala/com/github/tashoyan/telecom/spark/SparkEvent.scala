package com.github.tashoyan.telecom.spark

import java.sql.Timestamp

import com.github.tashoyan.telecom.event.{Event, WithEventInfo}
import com.github.tashoyan.telecom.util.Timestamps.RichTimestamp
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Spark-specific representation of [[Event]].
  * <p>
  * Spark Streaming functions like watermarking or time windows require
  * that the timestamp attribute has data type `java.sql.Timestamp`.
  *
  * @param timestamp Event timestamp.
  * @param siteId    Identifier of the site where the event occurred.
  * @param severity  Event severity.
  * @param info      Any textual information.
  */
case class SparkEvent(
    timestamp: Timestamp,
    siteId: Long,
    severity: String,
    override val info: String
) extends WithEventInfo {

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

  private val columns: Seq[String] = Seq(
    timestampColumn,
    siteIdColumn,
    severityColumn,
    infoColumn
  )

  implicit val defaultEventOrdering: Ordering[SparkEvent] = Ordering.by(_.timestamp)

  def apply(event: Event): SparkEvent =
    SparkEvent(
      new Timestamp(event.timestamp),
      event.siteId,
      event.severity,
      event.info
    )

  def isInCausalRelationship(cause: SparkEvent, consequence: SparkEvent, maxIntervalMillis: Long, minIntervalMillis: Long = 0): Boolean =
    consequence.timestamp.getTime - cause.timestamp.getTime >= minIntervalMillis &&
      consequence.timestamp.getTime - cause.timestamp.getTime <= maxIntervalMillis

  private def dataFrameAsEventDataset(df: DataFrame)(implicit spark: SparkSession): Dataset[SparkEvent] = {
    import spark.implicits._
    df.select(columns.head, columns.tail: _*)
      .as[SparkEvent]
  }

  implicit class SparkEventDataFrame(val df: DataFrame) extends AnyVal {

    @inline def asSparkEvents(implicit spark: SparkSession): Dataset[SparkEvent] = {
      dataFrameAsEventDataset(df)
    }

  }

}
