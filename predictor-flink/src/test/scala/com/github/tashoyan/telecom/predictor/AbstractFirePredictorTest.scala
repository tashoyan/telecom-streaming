package com.github.tashoyan.telecom.predictor

import java.sql.Timestamp

import com.github.tashoyan.telecom.event.{Alarm, Event}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStreamUtils, StreamExecutionEnvironment, _}
import org.apache.flink.test.util.AbstractTestBase
import org.junit.Test
import org.scalatest.Inside
import org.scalatest.Matchers._
import org.scalatest.junit.JUnitSuiteLike

abstract class AbstractFirePredictorTest extends AbstractTestBase with JUnitSuiteLike with Inside {

  protected val problemTimeoutMillis = 1000L
  protected val eventOutOfOrdernessMillis = 0L

  protected val siteId = 1L
  protected val eventSeverity = "MAJOR"
  protected val heatInfo = "Heat event"
  protected val smokeInfo = "Smoke event"
  protected val alarmSeverity = "CRITICAL"

  protected def firePredictor: FlinkFirePredictor = firePredictor(problemTimeoutMillis, eventOutOfOrdernessMillis)

  protected def firePredictor(
      problemTimeoutMillis: Long,
      eventOutOfOrdernessMillis: Long
  ): FlinkFirePredictor

  /*
  + [heat]
  + [smoke]
  + [heat, smoke]
  + same timestamp: [heat, smoke]
  + at problem timeout: [heat, smoke]
  + [heat], [smoke]
  + [heat, heat, smoke]
  + [heat], [heat, smoke]
  + [heat], [smoke, heat]
  + [heat, smoke, heat]
  + [smoke, smoke, heat]
  + [smoke, smoke, heat, smoke]
  + now: [smoke, smoke, heat, smoke]
  */

  @Test def singleHeat(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val events = env.fromElements(
      Event(timestamp = 500L, siteId, eventSeverity, heatInfo)
    )

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
      Event(timestamp = 500L, siteId, eventSeverity, smokeInfo)
    )

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
      Event(timestamp = 0L, siteId, eventSeverity, heatInfo),
      Event(timestamp = 500L, siteId, eventSeverity, smokeInfo)
    )

    val alarms = firePredictor.predictAlarms(events)

    val result = new DataStreamUtils(alarms)
      .collect()
      .toList
    result should have length 1
    val alarm = result.head
    inside(alarm) { case Alarm(timestamp, objectId, severity, info) =>
      timestamp should be(new Timestamp(500L))
      objectId should be(siteId)
      severity should be(alarmSeverity)
      info should startWith(s"Fire on site $siteId")
      info should include regex s"(?i)first\\s+heat\\s+at\\s+"
      info should include(new Timestamp(0L).toString)
    }
    ()
  }

  @Test def sameTimestampHeatSmoke(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val events = env.fromElements(
      Event(timestamp = 0L, siteId, eventSeverity, heatInfo),
      Event(timestamp = 0L, siteId, eventSeverity, smokeInfo)
    )

    val alarms = firePredictor.predictAlarms(events)

    val result = new DataStreamUtils(alarms)
      .collect()
      .toList
    result shouldBe empty
    ()
  }

  @Test def atProblemTimeoutHeatSmoke(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val events = env.fromElements(
      Event(timestamp = 0L, siteId, eventSeverity, heatInfo),
      Event(timestamp = problemTimeoutMillis, siteId, eventSeverity, smokeInfo)
    )

    val alarms = firePredictor.predictAlarms(events)

    val result = new DataStreamUtils(alarms)
      .collect()
      .toList
    result shouldBe empty
    ()
  }

  @Test def heatPauseSmoke(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val events = env.fromElements(
      Event(timestamp = 0L, siteId, eventSeverity, heatInfo),
      Event(timestamp = 1500L, siteId, eventSeverity, smokeInfo)
    )

    val alarms = firePredictor.predictAlarms(events)

    val result = new DataStreamUtils(alarms)
      .collect()
      .toList
    result shouldBe empty
    ()
  }

  @Test def heatHeatSmoke(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val events = env.fromElements(
      Event(timestamp = 0L, siteId, eventSeverity, heatInfo),
      Event(timestamp = 500L, siteId, eventSeverity, heatInfo),
      Event(timestamp = 800L, siteId, eventSeverity, smokeInfo)
    )

    val alarms = firePredictor.predictAlarms(events)

    val result = new DataStreamUtils(alarms)
      .collect()
      .toList
    result should have length 1
    val alarm = result.head
    inside(alarm) { case Alarm(timestamp, objectId, severity, info) =>
      timestamp should be(new Timestamp(800L))
      objectId should be(siteId)
      severity should be(alarmSeverity)
      info should startWith(s"Fire on site $siteId")
      info should include regex s"(?i)first\\s+heat\\s+at\\s+"
      info should include(new Timestamp(0L).toString)
    }
    ()
  }

  @Test def heatPauseHeatSmoke(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val events = env.fromElements(
      Event(timestamp = 0L, siteId, eventSeverity, heatInfo),
      Event(timestamp = 1500L, siteId, eventSeverity, heatInfo),
      Event(timestamp = 1800L, siteId, eventSeverity, smokeInfo)
    )

    val alarms = firePredictor.predictAlarms(events)

    val result = new DataStreamUtils(alarms)
      .collect()
      .toList
    result should have length 1
    val alarm = result.head
    inside(alarm) { case Alarm(timestamp, objectId, severity, info) =>
      timestamp should be(new Timestamp(1800L))
      objectId should be(siteId)
      severity should be(alarmSeverity)
      info should startWith(s"Fire on site $siteId")
      info should include regex s"(?i)first\\s+heat\\s+at\\s+"
      info should include(new Timestamp(1500L).toString)
    }
    ()
  }

  @Test def heatPauseSmokeHeat(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val events = env.fromElements(
      Event(timestamp = 0L, siteId, eventSeverity, heatInfo),
      Event(timestamp = 1500L, siteId, eventSeverity, smokeInfo),
      Event(timestamp = 1800L, siteId, eventSeverity, heatInfo)
    )

    val alarms = firePredictor.predictAlarms(events)

    val result = new DataStreamUtils(alarms)
      .collect()
      .toList
    result shouldBe empty
    ()
  }

  @Test def heatSmokeHeat(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val events = env.fromElements(
      Event(timestamp = 0L, siteId, eventSeverity, heatInfo),
      Event(timestamp = 500L, siteId, eventSeverity, smokeInfo),
      Event(timestamp = 800L, siteId, eventSeverity, heatInfo)
    )

    val alarms = firePredictor.predictAlarms(events)

    val result = new DataStreamUtils(alarms)
      .collect()
      .toList
    result should have length 1
    val alarm = result.head
    inside(alarm) { case Alarm(timestamp, objectId, severity, info) =>
      timestamp should be(new Timestamp(500L))
      objectId should be(siteId)
      severity should be(alarmSeverity)
      info should startWith(s"Fire on site $siteId")
      info should include regex s"(?i)first\\s+heat\\s+at\\s+"
      info should include(new Timestamp(0L).toString)
    }
    ()
  }

  @Test def smokeSmokeHeat(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val events = env.fromElements(
      Event(timestamp = 0L, siteId, eventSeverity, smokeInfo),
      Event(timestamp = 500L, siteId, eventSeverity, smokeInfo),
      Event(timestamp = 800L, siteId, eventSeverity, heatInfo)
    )

    val alarms = firePredictor.predictAlarms(events)

    val result = new DataStreamUtils(alarms)
      .collect()
      .toList
    result shouldBe empty
    ()
  }

  @Test def smokeSmokeHeatSmoke(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val events = env.fromElements(
      Event(timestamp = 0L, siteId, eventSeverity, smokeInfo),
      Event(timestamp = 500L, siteId, eventSeverity, smokeInfo),
      Event(timestamp = 800L, siteId, eventSeverity, heatInfo),
      Event(timestamp = 900L, siteId, eventSeverity, smokeInfo)
    )

    val alarms = firePredictor.predictAlarms(events)

    val result = new DataStreamUtils(alarms)
      .collect()
      .toList
    result should have length 1
    val alarm = result.head
    inside(alarm) { case Alarm(timestamp, objectId, severity, info) =>
      timestamp should be(new Timestamp(900L))
      objectId should be(siteId)
      severity should be(alarmSeverity)
      info should startWith(s"Fire on site $siteId")
      info should include regex s"(?i)first\\s+heat\\s+at\\s+"
      info should include(new Timestamp(800L).toString)
    }
    ()
  }

  @Test def nowSmokeSmokeHeatSmoke(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val zero = System.currentTimeMillis()
    val events = env.fromElements(
      Event(timestamp = zero, siteId, eventSeverity, smokeInfo),
      Event(timestamp = zero + 500L, siteId, eventSeverity, smokeInfo),
      Event(timestamp = zero + 800L, siteId, eventSeverity, heatInfo),
      Event(timestamp = zero + 900L, siteId, eventSeverity, smokeInfo)
    )

    val alarms = firePredictor(problemTimeoutMillis, eventOutOfOrdernessMillis = 10000L)
      .predictAlarms(events)

    val result = new DataStreamUtils(alarms)
      .collect()
      .toList
    result should have length 1
    val alarm = result.head
    inside(alarm) { case Alarm(timestamp, objectId, severity, info) =>
      timestamp should be(new Timestamp(zero + 900L))
      objectId should be(siteId)
      severity should be(alarmSeverity)
      info should startWith(s"Fire on site $siteId")
      info should include regex s"(?i)first\\s+heat\\s+at\\s+"
      info should include(new Timestamp(zero + 800L).toString)
    }
    ()
  }

}
