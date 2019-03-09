package com.github.tashoyan.telecom.event

case class Event(
    timestamp: Long,
    siteId: Long,
    severity: String,
    override val info: String
) extends WithEventInfo

object Event {
  val timestampColumn = "timestamp"
  val siteIdColumn = "siteId"
  val severityColumn = "severity"
  val infoColumn = "info"

  implicit val defaultEventOrdering: Ordering[Event] = Ordering.by(_.timestamp)

  def isInCausalRelationship(cause: Event, consequence: Event, maxIntervalMillis: Long, minIntervalMillis: Long = 0): Boolean =
    consequence.timestamp - cause.timestamp >= minIntervalMillis &&
      consequence.timestamp - cause.timestamp <= maxIntervalMillis

}
