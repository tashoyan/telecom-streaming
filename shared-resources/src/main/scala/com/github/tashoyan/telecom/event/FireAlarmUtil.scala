package com.github.tashoyan.telecom.event

object FireAlarmUtil {

  def isFireCandidate(event: Event): Boolean =
    event.isHeat || event.isSmoke

  val fireAlarmSeverity: String = "CRITICAL"

}
