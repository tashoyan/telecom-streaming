package com.github.tashoyan.telecom.predictor

import java.sql.Timestamp

import _root_.org.junit.Test
import com.github.tashoyan.telecom.event.{Alarm, Event}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.test.util.AbstractTestBase
import org.scalatest.Inside
import org.scalatest.Matchers._
import org.scalatest.junit.JUnitSuiteLike

class SessionWindowFirePredictorTest extends AbstractTestBase with JUnitSuiteLike with Inside {

  private val problemTimeoutMillis = 1000L
  private val eventOutOfOrdernessMillis = 2000L

  private val siteId = 1L
  private val eventSeverity = "MAJOR"
  private val heatInfo = "Heat event"
  private val smokeInfo = "Smoke event"

  /*
  + [heat]
  + [smoke]
  + [heat, smoke]
  - [heat], [smoke]
  - [heat, heat, smoke]
  - [heat], [heat, smoke]
  - [heat], [smoke, heat]
  - [heat, smoke, heat]
  - [smoke, smoke, heat]
  - [smoke, smoke, heat, smoke]
  */

  @Test def singleHeat(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val events = env.fromElements(
      Event(timestamp = new Timestamp(500L), siteId, eventSeverity, heatInfo)
    )

    val firePredictor = new SessionWindowFirePredictor(problemTimeoutMillis, eventOutOfOrdernessMillis)
    val alarms = firePredictor.predictAlarms(events)

    val result = new DataStreamUtils(alarms)
      .collect()
      .toList
    result shouldBe empty
    ()
  }

  @Test def singleSmoke(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val events = env.fromElements(
      Event(timestamp = new Timestamp(500L), siteId, eventSeverity, smokeInfo)
    )

    val firePredictor = new SessionWindowFirePredictor(problemTimeoutMillis, eventOutOfOrdernessMillis)
    val alarms = firePredictor.predictAlarms(events)

    val result = new DataStreamUtils(alarms)
      .collect()
      .toList
    result shouldBe empty
    ()
  }

  @Test def heatSmoke(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val events = env.fromElements(
      Event(timestamp = new Timestamp(0L), siteId, eventSeverity, heatInfo),
      Event(timestamp = new Timestamp(500L), siteId, eventSeverity, smokeInfo)
    )

    val firePredictor = new SessionWindowFirePredictor(problemTimeoutMillis, eventOutOfOrdernessMillis)
    val alarms = firePredictor.predictAlarms(events)

    val result = new DataStreamUtils(alarms)
      .collect()
      .toList
    result should have length 1
    val alarm = result.head
    inside(alarm) { case Alarm(timestamp, objectId, severity, info) =>
      timestamp should be(new Timestamp(500L))
      objectId should be(siteId)
      severity should be("CRITICAL")
      info should startWith(s"Fire on site $siteId")
      info should include regex s"(?i)first\\s+heat\\s+at\\s+"
      info should include(new Timestamp(0L).toString)
    }
    ()
  }
  @Test def heatPauseSmoke(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val events = env.fromElements(
      Event(timestamp = new Timestamp(0L), siteId, eventSeverity, heatInfo),
      Event(timestamp = new Timestamp(1500L), siteId, eventSeverity, smokeInfo)
    )

    val firePredictor = new SessionWindowFirePredictor(problemTimeoutMillis, eventOutOfOrdernessMillis)
    val alarms = firePredictor.predictAlarms(events)

    val result = new DataStreamUtils(alarms)
      .collect()
      .toList
    result shouldBe empty
    ()
  }

}
