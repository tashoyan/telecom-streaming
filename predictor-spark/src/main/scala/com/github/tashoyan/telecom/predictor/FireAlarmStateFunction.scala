package com.github.tashoyan.telecom.predictor

import com.github.tashoyan.telecom.event.Event
import org.apache.spark.sql.streaming.GroupState

//TODO Enable scalastyle back
//scalastyle:off
class FireAlarmStateFunction(problemTimeoutMillis: Long) extends AlarmStateFunction {

  override def updateAlarmState(siteId: Long, siteEvents: Iterator[Event], state: GroupState[ProblemState]): Iterator[Alarm] = {
    if (!state.exists) {
      /* no heat yet; check for heat now */
      println(s"NEW STATE on $siteId")

      val fireAlarmState = NoneState()(problemTimeoutMillis)
        .transition(siteEvents)
      fireAlarmState match {
        case NoneState() =>
          //+ state exists [N] / state timed out [-] / heat [N] / smoke [N] / smoke-heat timeout [-]
          //+ state exists [N] / state timed out [-] / heat [N] / smoke [Y] / smoke-heat timeout [-]
          /* smoke is too late */
          //+ state exists [N] / state timed out [-] / heat [Y] / smoke [Y] / smoke-heat timeout [Y]
          //+ state exists [N] / state timed out [-] / heat [Y] multiple / smoke [Y] multiple / smoke-heat timeout [Y]
          Iterator.empty
        case HeatState(heatEvent) =>
          //+ state exists [N] / state timed out [-] / heat [Y] / smoke [N] / smoke-heat timeout [-]
          val newState = ProblemState(heatEvent)
          state.update(newState)
          val timeoutTimestamp = heatEvent.timestamp.getTime + problemTimeoutMillis
          state.setTimeoutTimestamp(timeoutTimestamp)
          Iterator.empty
        case HeatAndSmokeState(_, smokeEvent) =>
          /* smoke is soon after heat - fire alarm */
          //+ state exists [N] / state timed out [-] / heat [Y] / smoke [Y] / smoke-heat timeout [N]
          //+ state exists [N] / state timed out [-] / heat [Y] multiple / smoke [Y] multiple / smoke-heat timeout [N]
          val smokeTs = smokeEvent.timestamp
          val alarm = Alarm(smokeTs, siteId, "MAJOR", s"Fire on site $siteId")
          Iterator(alarm)
      }
    } else if (state.exists && !state.hasTimedOut) {
      /* already observed heat; checking now for smoke */
      println(s"EXISTING STATE on $siteId")

      val heatEvent = state.get.triggerEvent
      val heatTs = heatEvent.timestamp
      val timeoutTimestamp = heatTs.getTime + problemTimeoutMillis
      state.setTimeoutTimestamp(timeoutTimestamp)

      val fireAlarmState = HeatState(heatEvent)(problemTimeoutMillis)
        .transition(siteEvents)
      fireAlarmState match {
        case NoneState() =>
          /* smoke is too late */
          //+ state exists [Y] / state timed out [N] / heat [N] / smoke [Y] / smoke-heat timeout [Y]
          //+ state exists [Y] / state timed out [N] / heat [Y] / smoke [Y] / smoke-heat timeout [Y]
          state.remove()
          Iterator.empty
        case HeatState(_) =>
          /* no smoke yet */
          //+ state exists [Y] / state timed out [N] / heat [N] / smoke [N] / smoke-heat timeout [-]
          //+ state exists [Y] / state timed out [N] / heat [Y] / smoke [N] / smoke-heat timeout [-]
          Iterator.empty
        case HeatAndSmokeState(_, smokeEvent) =>
          /* smoke is soon after heat - fire alarm */
          //+ state exists [Y] / state timed out [N] / heat [N] / smoke [Y] / smoke-heat timeout [N]
          //+ state exists [Y] / state timed out [N] / heat [Y] / smoke [Y] / smoke-heat timeout [N]
          val smokeTs = smokeEvent.timestamp
          val alarm = Alarm(smokeTs, siteId, "MAJOR", s"Fire on site $siteId")
          state.remove()
          Iterator(alarm)
      }
    } else {
      //+ state exists [Y] / state timed out [Y] / heat [N] / smoke [N] / smoke-heat timeout [-]
      //+ state exists [Y] / state timed out [Y] / heat [Y] / smoke [N] / smoke-heat timeout [-]
      //+ state exists [Y] / state timed out [Y] / heat [N] / smoke [Y] / smoke-heat timeout [N|Y]
      //+ state exists [Y] / state timed out [Y] / heat [Y] / smoke [Y] / smoke-heat timeout [N|Y]
      /* problem state timed out */
      state.remove()
      Iterator.empty
    }
  }

}
