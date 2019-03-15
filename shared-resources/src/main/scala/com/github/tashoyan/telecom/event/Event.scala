package com.github.tashoyan.telecom.event

/**
  * Event.
  * <p>
  * <b>Deduplication</b>
  * <p>
  * Events are not supposed to have unique identifiers.
  * The proposed way to deduplicate them is by the pair: `(timestamp, siteId)`.
  *
  * @param timestamp Event timestamp - milliseconds since the Epoch.
  * @param siteId    Identifier of the site where the event occurred.
  * @param severity  Event severity.
  * @param info      Any textual information.
  */
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
