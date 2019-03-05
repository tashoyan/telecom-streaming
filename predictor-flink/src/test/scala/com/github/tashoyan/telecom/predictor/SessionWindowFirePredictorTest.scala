package com.github.tashoyan.telecom.predictor

import java.sql.Timestamp

import _root_.org.junit.Test
import com.github.tashoyan.telecom.event.Event
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.test.util.AbstractTestBase
import org.scalatest.junit.JUnitSuiteLike

class SessionWindowFirePredictorTest extends AbstractTestBase with JUnitSuiteLike {

  @Test def dummy(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val events = env.fromElements(
      Event(timestamp = new Timestamp(1L), siteId = 1L, severity = "MAJOR", info = "event 1"),
      Event(timestamp = new Timestamp(2L), siteId = 1L, severity = "MAJOR", info = "event 2")
    )

    val problemTimeoutMillis = 1000L
    val firePredictor = new SessionWindowFirePredictor(problemTimeoutMillis)
    val alarms = firePredictor.predictAlarms(events)

    val result = new DataStreamUtils(alarms).collect()
    result.foreach(println)
  }

}
