package com.github.tashoyan.telecom.event

import java.sql.Timestamp

case class Alarm(
    timestamp: Timestamp,
    objectId: Long,
    severity: String,
    info: String
)

object Alarm {
  val timestampColumn = "timestamp"
  val objectIdColumn = "objectId"
  val severityColumn = "severity"
  val infoColumn = "info"

  val columns: Set[String] = Set(
    timestampColumn,
    objectIdColumn,
    severityColumn,
    infoColumn
  )

}
