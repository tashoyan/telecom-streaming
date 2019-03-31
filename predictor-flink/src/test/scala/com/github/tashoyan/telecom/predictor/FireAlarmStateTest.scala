package com.github.tashoyan.telecom.predictor

import java.sql.Timestamp

import com.github.tashoyan.telecom.event.{Alarm, Event}
import org.scalatest.Matchers._
import org.scalatest.{FunSuite, Inside}

class FireAlarmStateTest extends FunSuite with Inside {
  protected val siteId = 1L
  protected val eventSeverity = "MAJOR"
  protected val heatInfo = "Heat event"
  protected val smokeInfo = "Smoke event"
  protected val alarmSeverity = "CRITICAL"

  test("heatSmokeHeat") {
    implicit val problemTimeoutMillis: Long = 1000L
    val fireAlarmState = new FireAlarmState()
      .update(Iterator(
        Event(800L, 1L, eventSeverity, heatInfo),
        Event(100L, 1L, eventSeverity, heatInfo),
        Event(500L, 1L, eventSeverity, smokeInfo)
      ))
    val alarm = fireAlarmState.getFireAlarm.get
    inside(alarm) { case Alarm(timestamp, objectId, severity, info) =>
      timestamp should be(new Timestamp(500L))
      objectId should be(siteId)
      severity should be(alarmSeverity)
      info should startWith(s"Fire on site $siteId")
      info should include regex s"(?i)first\\s+heat\\s+at\\s+"
      info should include(new Timestamp(100L).toString)
    }
  }

}
