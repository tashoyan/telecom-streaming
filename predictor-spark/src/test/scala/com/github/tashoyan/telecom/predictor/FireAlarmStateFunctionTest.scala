package com.github.tashoyan.telecom.predictor

import java.sql.Timestamp
import java.util.concurrent.TimeUnit

import com.github.tashoyan.telecom.event.Event
import org.apache.spark.sql.streaming.GroupState
import org.scalamock.scalatest.MockFactory
import org.scalatest.FunSuite

class FireAlarmStateFunctionTest extends FunSuite with MockFactory {

  private val problemTimeoutMillis = TimeUnit.SECONDS.toMillis(10)

  private val siteId = 1L
  private val severity = "MAJOR"
  private val heatInfo = "Heat on site"
  private val smokeInfo = "Smoke on site"

  def heatEvent(timestamp: Timestamp): Event =
    Event(timestamp, siteId, severity, heatInfo)

  def smokeEvent(timestamp: Timestamp): Event =
    Event(timestamp, siteId, severity, smokeInfo)

  test("state exists [N] / state timed out [-] / heat [N] / smoke [N] / smoke-heat timeout [-]") {
    val state: GroupState[ProblemState] = mock[GroupState[ProblemState]]
    (state.exists _)
      .expects()
      .atLeastOnce()
      .returns(false)

    val events = Iterator.empty

    val alarmStateFunction = new FireAlarmStateFunction(problemTimeoutMillis)

    val result = alarmStateFunction.updateAlarmState(siteId, events, state)
    assert(result.isEmpty, "Expected none alarms")
  }

  test("state exists [N] / state timed out [-] / heat [Y] / smoke [N] / smoke-heat timeout [-]") {
    val heatTimestamp = new Timestamp(1000L)

    val state: GroupState[ProblemState] = mock[GroupState[ProblemState]]
    inSequence {
      (state.exists _)
        .expects()
        .atLeastOnce()
        .returns(false)
      inAnyOrder {
        (state.update _)
          .expects(ProblemState(siteId, heatTimestamp))
          .once()
        (state.setTimeoutTimestamp(_: Long))
          .expects(heatTimestamp.getTime + problemTimeoutMillis)
          .once()
      }
    }

    val events = Iterator(heatEvent(heatTimestamp))

    val alarmStateFunction = new FireAlarmStateFunction(problemTimeoutMillis)

    val result = alarmStateFunction.updateAlarmState(siteId, events, state)
    assert(result.isEmpty, "Expected none alarms")
  }

}
