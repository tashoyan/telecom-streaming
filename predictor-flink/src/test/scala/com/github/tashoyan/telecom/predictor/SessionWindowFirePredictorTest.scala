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

  @Test def dummy(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val siteId = 1L
    val events = env.fromElements(
      Event(timestamp = new Timestamp(0L), siteId, severity = "MAJOR", info = "Heat 1"),
      Event(timestamp = new Timestamp(500L), siteId, severity = "MAJOR", info = "Smoke 1")
    )

    val problemTimeoutMillis = 1000L
    val eventOutOfOrdernessMillis = 5000L
    val firePredictor = new SessionWindowFirePredictor(problemTimeoutMillis, eventOutOfOrdernessMillis)
    val alarms = firePredictor.predictAlarms(events)

    val result = new DataStreamUtils(alarms)
      .collect()
      .toList
    println(s"Alarms (${result.size}): $result")
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
    result.foreach(println)
  }

}
