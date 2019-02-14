package com.github.tashoyan.telecom.predictor

import java.sql.Timestamp

import com.github.tashoyan.telecom.event.Event
import org.apache.spark.sql.streaming.GroupState

//TODO Enable scalastyle back
//scalastyle:off
class FireAlarmStateFunction(problemTimeoutMillis: Long) extends AlarmStateFunction {

  override def updateAlarmState(siteId: Long, siteEvents: Iterator[Event], state: GroupState[ProblemState]): Iterator[Alarm] = {
    if (!state.exists) {
      /* no heat yet; check for heat now */
      //TODO More than one heat from the same site
      println(s"NEW STATE on $siteId")
      val heatTimestamp = siteEvents.toStream
        .find(isHeatEvent)
        .map(_.timestamp)
      val smokeTimestamp = siteEvents.toStream
        .find(isSmokeEvent)
        .map(_.timestamp)
      println(s" -- heatTimestamp: $heatTimestamp, smokeTimestamp: $smokeTimestamp")
      //TODO smoke in the same batch as heat - is it really a realistic scenario?
      if (heatTimestamp.isDefined) {
        val heatTs = heatTimestamp.get
        if (smokeTimestamp.isDefined) {
          val smokeTs = smokeTimestamp.get
          if (isInTriggerInterval(heatTs, smokeTs)) {
            //+ state exists [N] / state timed out [-] / heat [Y] / smoke [Y] / smoke-heat timeout [N]
            /* smoke is soon after heat - fire alarm */
            val alarm = Alarm(smokeTs, siteId, "MAJOR", s"Fire on site $siteId")
            Iterator(alarm)
          } else {
            //+ state exists [N] / state timed out [-] / heat [Y] / smoke [Y] / smoke-heat timeout [Y]
            /* smoke is too late */
            println(s" -- smoke is too late")
            Iterator.empty
          }
        } else {
          //+ state exists [N] / state timed out [-] / heat [Y] / smoke [N] / smoke-heat timeout [-]
          val newState = ProblemState(siteId, heatTs)
          state.update(newState)
          val timeoutTimestamp = heatTs.getTime + problemTimeoutMillis
          println(s" -- timeoutTimestamp: ${new Timestamp(timeoutTimestamp)}")
          state.setTimeoutTimestamp(timeoutTimestamp)
          Iterator.empty
        }
      } else {
        //+ state exists [N] / state timed out [-] / heat [N] / smoke [N] / smoke-heat timeout [-]
        //+ state exists [N] / state timed out [-] / heat [N] / smoke [Y] / smoke-heat timeout [-]
        Iterator.empty
      }
    } else if (state.exists && !state.hasTimedOut) {
      //TODO More than one smoke from the same site
      println(s"EXISTING STATE on $siteId")
      /* already observed heat; checking now for smoke */
      val heatTs = state.get.heatTimestamp
      println(s" -- heatTimestamp: $heatTs")
      val timeoutTimestamp = heatTs.getTime + problemTimeoutMillis
      println(s" -- timeoutTimestamp: ${new Timestamp(timeoutTimestamp)}")
      state.setTimeoutTimestamp(timeoutTimestamp)

      val smokeTimestamp = siteEvents.toStream
        .find(isSmokeEvent)
        .map(_.timestamp)
      println(s" -- smokeTimestamp: $smokeTimestamp")
      if (smokeTimestamp.isDefined) {
        val smokeTs = smokeTimestamp.get
        if (isInTriggerInterval(heatTs, smokeTs)) {
          /* smoke is soon after heat - fire alarm */
          val alarm = Alarm(smokeTs, siteId, "MAJOR", s"Fire on site $siteId")
          state.remove()
          Iterator(alarm)
        } else {
          /* smoke is too late */
          println(s" -- smoke is too late")
          state.remove()
          Iterator.empty
        }
      } else {
        /* no smoke yet */
        println(s" -- no smoke yet")
        Iterator.empty
      }
    } else {
      /* problem state timed out */
      state.remove()
      Iterator.empty
    }
  }

  def isHeatEvent(event: Event): Boolean =
    event.info != null &&
      event.info.toLowerCase.contains("heat")

  def isSmokeEvent(event: Event): Boolean =
    event.info != null &&
      event.info.toLowerCase.contains("smoke")

  def isInTriggerInterval(heatTimestamp: Timestamp, smokeTimestamp: Timestamp): Boolean =
    smokeTimestamp.getTime - heatTimestamp.getTime >= 0 &&
      smokeTimestamp.getTime - heatTimestamp.getTime <= problemTimeoutMillis

}
