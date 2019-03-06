package com.github.tashoyan.telecom.predictor

import com.github.tashoyan.telecom.event.{Alarm, Event, ProblemState}
import org.apache.spark.sql.streaming.GroupState

class FireAlarmStateFunction(problemTimeoutMillis: Long) extends AlarmStateFunction {
  private val alarmSeverity = "CRITICAL"

  override def updateAlarmState(siteId: Long, siteEvents: Iterator[Event], groupState: GroupState[ProblemState]): Iterator[Alarm] = {
    if (!groupState.exists) {
      /* no heat yet; check for heat now */
      handleNonExistingState(siteId, siteEvents, groupState)
        .iterator
    } else if (groupState.exists && !groupState.hasTimedOut) {
      /* already observed heat; checking now for smoke */
      handleExistingState(siteId, siteEvents, groupState)
        .iterator
    } else {
      handleTimedOutState(groupState)
        .iterator
    }
  }

  private def handleNonExistingState(siteId: Long, siteEvents: Iterator[Event], groupState: GroupState[ProblemState]): Option[Alarm] = {
    val fireAlarmState = NoneState()(problemTimeoutMillis)
      .transition(siteEvents)
    fireAlarmState match {
      case NoneState() =>
        //+ state exists [N] / state timed out [-] / heat [N] / smoke [N] / smoke-heat timeout [-]
        //+ state exists [N] / state timed out [-] / heat [N] / smoke [Y] / smoke-heat timeout [-]
        /* smoke is too late */
        //+ state exists [N] / state timed out [-] / heat [Y] / smoke [Y] / smoke-heat timeout [Y]
        //+ state exists [N] / state timed out [-] / heat [Y] multiple / smoke [Y] multiple / smoke-heat timeout [Y]
        None
      case HeatState(heatEvent) =>
        //+ state exists [N] / state timed out [-] / heat [Y] / smoke [N] / smoke-heat timeout [-]
        val newState = ProblemState(heatEvent)
        groupState.update(newState)
        val timeoutTimestamp = heatEvent.timestamp.getTime + problemTimeoutMillis
        groupState.setTimeoutTimestamp(timeoutTimestamp)
        None
      case HeatAndSmokeState(heatEvent, smokeEvent) =>
        /* smoke is soon after heat - fire alarm */
        //+ state exists [N] / state timed out [-] / heat [Y] / smoke [Y] / smoke-heat timeout [N]
        //+ state exists [N] / state timed out [-] / heat [Y] multiple / smoke [Y] multiple / smoke-heat timeout [N]
        val smokeTs = smokeEvent.timestamp
        val alarm = Alarm(smokeTs, siteId, alarmSeverity, s"Fire on site $siteId. First heat at ${heatEvent.timestamp}.")
        Some(alarm)
    }
  }

  private def handleExistingState(siteId: Long, siteEvents: Iterator[Event], groupState: GroupState[ProblemState]): Option[Alarm] = {
    val heatEvent0 = groupState.get.triggerEvent
    val fireAlarmState = HeatState(heatEvent0)(problemTimeoutMillis)
      .transition(siteEvents)
    fireAlarmState match {
      case NoneState() =>
        /* smoke is too late */
        //+ state exists [Y] / state timed out [N] / heat [N] / smoke [Y] / smoke-heat timeout [Y]
        //+ state exists [Y] / state timed out [N] / heat [Y] / smoke [Y] / smoke-heat timeout [Y]
        //+ state exists [Y] / state timed out [N] / heat [Y] multiple / smoke [Y] multiple / smoke-heat timeout [Y]
        groupState.remove()
        None
      case HeatState(heatEvent) =>
        /* no smoke yet */
        //+ state exists [Y] / state timed out [N] / heat [N] / smoke [N] / smoke-heat timeout [-]
        //+ state exists [Y] / state timed out [N] / heat [Y] / smoke [N] / smoke-heat timeout [-]
        val newState = ProblemState(heatEvent)
        groupState.update(newState)
        val timeoutTimestamp = heatEvent.timestamp.getTime + problemTimeoutMillis
        groupState.setTimeoutTimestamp(timeoutTimestamp)
        None
      case HeatAndSmokeState(heatEvent, smokeEvent) =>
        /* smoke is soon after heat - fire alarm */
        //+ state exists [Y] / state timed out [N] / heat [N] / smoke [Y] / smoke-heat timeout [N]
        //+ state exists [Y] / state timed out [N] / heat [Y] / smoke [Y] / smoke-heat timeout [N]
        //+ state exists [Y] / state timed out [N] / heat [Y] multiple / smoke [Y] multiple / smoke-heat timeout [N]
        val smokeTs = smokeEvent.timestamp
        val alarm = Alarm(smokeTs, siteId, alarmSeverity, s"Fire on site $siteId. First heat at ${heatEvent.timestamp}.")
        groupState.remove()
        Some(alarm)
    }
  }

  private def handleTimedOutState(groupState: GroupState[ProblemState]): Option[Alarm] = {
    /* problem state timed out */
    //+ state exists [Y] / state timed out [Y] / heat [N] / smoke [N] / smoke-heat timeout [-]
    //+ state exists [Y] / state timed out [Y] / heat [Y] / smoke [N] / smoke-heat timeout [-]
    //+ state exists [Y] / state timed out [Y] / heat [N] / smoke [Y] / smoke-heat timeout [N|Y]
    //+ state exists [Y] / state timed out [Y] / heat [Y] / smoke [Y] / smoke-heat timeout [N|Y]
    groupState.remove()
    None
  }

}
