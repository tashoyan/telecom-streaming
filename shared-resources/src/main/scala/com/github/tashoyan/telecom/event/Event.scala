package com.github.tashoyan.telecom.event

import java.sql.Timestamp
import com.github.tashoyan.telecom.util.Timestamps.RichTimestamp

case class Event(
    timestamp: Timestamp,
    siteId: Long,
    severity: String,
    info: String
) {

  def isCommunication: Boolean =
    info != null &&
      info.toLowerCase.contains("communication")

  def isHeat: Boolean =
    info != null &&
      info.toLowerCase.contains("heat")

  def isSmoke: Boolean =
    info != null &&
      info.toLowerCase.contains("smoke")

}

object Event {
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

  implicit val defaultEventOrdering: Ordering[Event] = Ordering.by(_.timestamp)

  def isInCausalRelationship(cause: Event, consequence: Event, maxIntervalMillis: Long, minIntervalMillis: Long = 0): Boolean =
    consequence.timestamp.getTime - cause.timestamp.getTime >= minIntervalMillis &&
      consequence.timestamp.getTime - cause.timestamp.getTime <= maxIntervalMillis

}
