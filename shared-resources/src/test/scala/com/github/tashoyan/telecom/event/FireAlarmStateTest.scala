package com.github.tashoyan.telecom.event

import java.sql.Timestamp

import org.scalatest.Matchers._
import org.scalatest.{FunSuite, Inside}

class FireAlarmStateTest extends FunSuite with Inside {
  private implicit val problemTimeoutMillis: Long = 1000L
  private val siteId = 1L
  private val eventSeverity = "MAJOR"
  private val heatInfo = "Heat event"
  private val smokeInfo = "Smoke event"
  private val alarmSeverity = "CRITICAL"

  test("heatSmokeHeat") {
    val fireAlarmState = new FireAlarmState()
      .update(Iterator(
        Event(800L, siteId, eventSeverity, heatInfo),
        Event(100L, siteId, eventSeverity, heatInfo),
        Event(500L, siteId, eventSeverity, smokeInfo)
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

  test("smokeSmokeHeatSmoke") {
    val fireAlarmState = new FireAlarmState()
      .update(Iterator(
        Event(timestamp = 100L, siteId, eventSeverity, smokeInfo),
        Event(timestamp = 500L, siteId, eventSeverity, smokeInfo),
        Event(timestamp = 800L, siteId, eventSeverity, heatInfo),
        Event(timestamp = 900L, siteId, eventSeverity, smokeInfo)
      ))
    val alarm = fireAlarmState.getFireAlarm.get
    inside(alarm) { case Alarm(timestamp, objectId, severity, info) =>
      timestamp should be(new Timestamp(900L))
      objectId should be(siteId)
      severity should be(alarmSeverity)
      info should startWith(s"Fire on site $siteId")
      info should include regex s"(?i)first\\s+heat\\s+at\\s+"
      info should include(new Timestamp(800L).toString)
    }
  }

}
