package com.github.tashoyan.telecom.event

import java.sql.Timestamp

case class Event(
    id: Long,
    timestamp: Timestamp,
    siteId: Long,
    info: String
)
