package com.github.tashoyan.telecom.event

import java.sql.Timestamp

case class Alarm(
    timestamp: Timestamp,
    siteId: Long,
    severity: String,
    info: String
)

object Alarm {
  val timestampColumn = "timestamp"
  val siteIdColumn = "siteId"
  val severityColumn = "severity"
  val infoColumn = "info"

  val columns: Set[String] = Set(
    timestampColumn,
    siteIdColumn,
    severityColumn,
    infoColumn
  )

}
