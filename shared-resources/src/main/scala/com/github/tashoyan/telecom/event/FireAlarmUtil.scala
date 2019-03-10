package com.github.tashoyan.telecom.event

import java.sql.Timestamp

object FireAlarmUtil {

  def isFireCandidate(event: WithEventInfo): Boolean =
    event.isHeat || event.isSmoke

  val fireAlarmSeverity: String = "CRITICAL"

  def createFireAlarm(heatEvent: Event, smokeEvent: Event): Alarm =
    Alarm(
      new Timestamp(smokeEvent.timestamp),
      smokeEvent.siteId,
      fireAlarmSeverity,
      s"Fire on site ${smokeEvent.siteId}. First heat at ${new Timestamp(heatEvent.timestamp)}."
    )

}
