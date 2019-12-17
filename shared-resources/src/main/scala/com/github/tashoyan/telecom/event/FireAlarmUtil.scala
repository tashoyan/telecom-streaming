package com.github.tashoyan.telecom.event

import com.github.tashoyan.telecom.util.Timestamps.formattedTimestamp

object FireAlarmUtil {

  def isFireCandidate(event: WithEventInfo): Boolean =
    event.isHeat || event.isSmoke

  val fireAlarmSeverity: String = "CRITICAL"

  def createFireAlarm(heatEvent: Event, smokeEvent: Event): Alarm =
    Alarm(
      smokeEvent.timestamp,
      smokeEvent.siteId,
      fireAlarmSeverity,
      s"Fire on site ${smokeEvent.siteId}. First heat at ${formattedTimestamp(heatEvent.timestamp)}."
    )

}
