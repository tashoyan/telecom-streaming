package com.github.tashoyan.telecom.event

import java.sql.Timestamp

case class Event(
    id: Long,
    timestamp: Timestamp,
    siteId: Long,
    severity: String,
    info: String
)

object Event {
  val idColumn = "id"
  val timestampColumn = "timestamp"
  val siteIdColumn = "siteId"
  val severityColumn = "severity"
  val infoColumn = "info"

  val keyColumn = "key"
  val valueColumn = "value"
}
