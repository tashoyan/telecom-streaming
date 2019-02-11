package com.github.tashoyan.telecom.predictor

import java.sql.Timestamp

import com.github.tashoyan.telecom.event.Event
import org.apache.spark.sql.streaming.GroupState

//TODO Enable scalastyle back
//scalastyle:off
class FireAlarmStateFunction(problemTimeoutMillis: Long) extends AlarmStateFunction {

  override def updateAlarmState(siteId: Long, siteEvents: Iterator[Event], state: GroupState[ProblemState]): Iterator[Alarm] = {
    if (state.hasTimedOut) {
      /* problem state timed out */
      state.remove()
      Iterator.empty
    } else if (state.exists) {
      println(s"EXISTING STATE on $siteId")
      /* already observed heat; checking now for smoke */
      val heatTimestamp = state.get.heatTimestamp
      println(s" -- heatTimestamp: $heatTimestamp")
      val timeoutTimestamp = heatTimestamp.getTime + problemTimeoutMillis
      println(s" -- timeoutTimestamp: ${new Timestamp(timeoutTimestamp)}")
      state.setTimeoutTimestamp(timeoutTimestamp)

      val smokeTimestamp = siteEvents.toStream
        .find(isSmokeEvent)
        .map(_.timestamp)
      println(s" -- smokeTimestamp: $smokeTimestamp")
      if (smokeTimestamp.isDefined) {
        val smokeTs = smokeTimestamp.get
        if (smokeTs.getTime - heatTimestamp.getTime > 0 &&
          smokeTs.getTime - heatTimestamp.getTime <= problemTimeoutMillis) {
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
      /* no heat yet; check for heat now */
      //TODO Missing fire alarm when smoke comes in the same batch with heat
      println(s"NEW STATE on $siteId")
      val heatTimestamp = siteEvents.toStream
        .find(isHeatEvent)
        .map(_.timestamp)
      println(s" -- heatTimestamp: $heatTimestamp")
      heatTimestamp.foreach { heatTs =>
        val newState = ProblemState(siteId, heatTs)
        state.update(newState)
        val timeoutTimestamp = heatTs.getTime + problemTimeoutMillis
        println(s" -- timeoutTimestamp: ${new Timestamp(timeoutTimestamp)}")
        state.setTimeoutTimestamp(timeoutTimestamp)
      }
      Iterator.empty
    }
  }

  def isHeatEvent(event: Event): Boolean =
    event.info != null &&
      event.info.toLowerCase.contains("heat")

  def isSmokeEvent(event: Event): Boolean =
    event.info != null &&
      event.info.toLowerCase.contains("smoke")

}
