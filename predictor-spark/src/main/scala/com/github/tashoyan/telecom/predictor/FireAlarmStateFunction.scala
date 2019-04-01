package com.github.tashoyan.telecom.predictor

import com.github.tashoyan.telecom.event.{Alarm, FireAlarmState}
import com.github.tashoyan.telecom.spark.SparkEvent
import org.apache.spark.sql.streaming.GroupState

class FireAlarmStateFunction(problemTimeoutMillis: Long) extends AlarmStateFunction {

  override def updateAlarmState(siteId: Long, siteEvents: Iterator[SparkEvent], groupState: GroupState[SparkFireAlarmState]): Iterator[Alarm] = {
    if (!groupState.exists) {
      /* no heat yet; check for heat now */
      handleNonExistingState(siteEvents, groupState)
        .iterator
    } else if (groupState.exists && !groupState.hasTimedOut) {
      /* already observed heat; checking now for smoke */
      handleExistingState(siteEvents, groupState)
        .iterator
    } else {
      handleTimedOutState(groupState)
        .iterator
    }
  }

  private def handleNonExistingState(siteEvents: Iterator[SparkEvent], groupState: GroupState[SparkFireAlarmState]): Option[Alarm] = {
    //+ state exists [N] / state timed out [-] / heat [N] / smoke [N] / smoke-heat timeout [-]
    //+ state exists [N] / state timed out [-] / heat [N] / smoke [Y] / smoke-heat timeout [-]
    /* smoke is too late */
    //+ state exists [N] / state timed out [-] / heat [Y] / smoke [Y] / smoke-heat timeout [Y]
    //+ state exists [N] / state timed out [-] / heat [Y] multiple / smoke [Y] multiple / smoke-heat timeout [Y]
    //+ state exists [N] / state timed out [-] / heat [Y] / smoke [N] / smoke-heat timeout [-]
    /* smoke is soon after heat - fire alarm */
    //+ state exists [N] / state timed out [-] / heat [Y] / smoke [Y] / smoke-heat timeout [N]
    //+ state exists [N] / state timed out [-] / heat [Y] multiple / smoke [Y] multiple / smoke-heat timeout [N]
    if (siteEvents.isEmpty)
      None
    else {
      val fireAlarmState = new FireAlarmState()(problemTimeoutMillis)
        .update(siteEvents.map(_.toEvent))
      val fireAlarm = fireAlarmState.getFireAlarm
      if (fireAlarm.isEmpty) {
        groupState.update(SparkFireAlarmState(fireAlarmState))
        val timeoutTimestamp = fireAlarmState.maxTimestamp.get + problemTimeoutMillis
        groupState.setTimeoutTimestamp(timeoutTimestamp)
      }
      fireAlarm
    }
  }

  private def handleExistingState(siteEvents: Iterator[SparkEvent], groupState: GroupState[SparkFireAlarmState]): Option[Alarm] = {
    /* smoke is too late */
    //+ state exists [Y] / state timed out [N] / heat [N] / smoke [Y] / smoke-heat timeout [Y]
    //+ state exists [Y] / state timed out [N] / heat [Y] / smoke [Y] / smoke-heat timeout [Y]
    //+ state exists [Y] / state timed out [N] / heat [Y] multiple / smoke [Y] multiple / smoke-heat timeout [Y]
    /* no smoke yet */
    //+ state exists [Y] / state timed out [N] / heat [N] / smoke [N] / smoke-heat timeout [-]
    //+ state exists [Y] / state timed out [N] / heat [Y] / smoke [N] / smoke-heat timeout [-]
    /* smoke is soon after heat - fire alarm */
    //+ state exists [Y] / state timed out [N] / heat [N] / smoke [Y] / smoke-heat timeout [N]
    //+ state exists [Y] / state timed out [N] / heat [Y] / smoke [Y] / smoke-heat timeout [N]
    //+ state exists [Y] / state timed out [N] / heat [Y] multiple / smoke [Y] multiple / smoke-heat timeout [N]
    if (siteEvents.isEmpty) {
      val fireAlarmState = groupState.get
        .toFireAlarmState
      groupState.update(SparkFireAlarmState(fireAlarmState))
      val timeoutTimestamp = fireAlarmState.maxTimestamp.get + problemTimeoutMillis
      groupState.setTimeoutTimestamp(timeoutTimestamp)
      None
    } else {
      val fireAlarmState = groupState.get
        .toFireAlarmState
        .update(siteEvents.map(_.toEvent))
      val fireAlarm = fireAlarmState.getFireAlarm
      if (fireAlarm.isEmpty) {
        groupState.update(SparkFireAlarmState(fireAlarmState))
        val timeoutTimestamp = fireAlarmState.maxTimestamp.get + problemTimeoutMillis
        groupState.setTimeoutTimestamp(timeoutTimestamp)
      } else {
        groupState.remove()
      }
      fireAlarm
    }
  }

  private def handleTimedOutState(groupState: GroupState[SparkFireAlarmState]): Option[Alarm] = {
    /* problem state timed out */
    //+ state exists [Y] / state timed out [Y] / heat [N] / smoke [N] / smoke-heat timeout [-]
    //+ state exists [Y] / state timed out [Y] / heat [Y] / smoke [N] / smoke-heat timeout [-]
    //+ state exists [Y] / state timed out [Y] / heat [N] / smoke [Y] / smoke-heat timeout [N|Y]
    //+ state exists [Y] / state timed out [Y] / heat [Y] / smoke [Y] / smoke-heat timeout [N|Y]
    groupState.remove()
    None
  }

}
