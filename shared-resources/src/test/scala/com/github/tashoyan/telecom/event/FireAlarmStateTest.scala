package com.github.tashoyan.telecom.event

import com.github.tashoyan.telecom.util.Timestamps.formattedTimestamp
import org.scalatest.Inside
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class FireAlarmStateTest extends AnyFunSuite with Matchers with Inside {
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
      timestamp shouldBe 500L
      objectId shouldBe siteId
      severity shouldBe alarmSeverity
      info should startWith(s"Fire on site $siteId")
      info should include regex s"(?i)first\\s+heat\\s+at\\s+"
      info should include(formattedTimestamp(100L))
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
      timestamp shouldBe 900L
      objectId shouldBe siteId
      severity shouldBe alarmSeverity
      info should startWith(s"Fire on site $siteId")
      info should include regex s"(?i)first\\s+heat\\s+at\\s+"
      info should include(formattedTimestamp(800L))
    }
  }

  test("maxTimestamp - heatEvents non-empty, smoke events non-empty") {
    val fireAlarmState = new FireAlarmState()
      .update(Iterator(
        Event(timestamp = 100L, siteId, eventSeverity, smokeInfo),
        Event(timestamp = 500L, siteId, eventSeverity, heatInfo),
        Event(timestamp = 800L, siteId, eventSeverity, heatInfo),
        Event(timestamp = 900L, siteId, eventSeverity, smokeInfo)
      ))
    val result = fireAlarmState.maxTimestamp
    result shouldBe Some(900L)
  }

  test("maxTimestamp - heatEvents empty, smoke events non-empty") {
    val fireAlarmState = new FireAlarmState()
      .update(Iterator(
        Event(timestamp = 100L, siteId, eventSeverity, smokeInfo),
        Event(timestamp = 900L, siteId, eventSeverity, smokeInfo)
      ))
    val result = fireAlarmState.maxTimestamp
    result shouldBe Some(900L)
  }

  test("maxTimestamp - heatEvents non-empty, smoke events empty") {
    val fireAlarmState = new FireAlarmState()
      .update(Iterator(
        Event(timestamp = 500L, siteId, eventSeverity, heatInfo),
        Event(timestamp = 800L, siteId, eventSeverity, heatInfo)
      ))
    val result = fireAlarmState.maxTimestamp
    result shouldBe Some(800L)
  }

  test("maxTimestamp - heatEvents empty, smoke events empty") {
    val fireAlarmState = new FireAlarmState()
    val result = fireAlarmState.maxTimestamp
    result shouldBe None
  }

}
