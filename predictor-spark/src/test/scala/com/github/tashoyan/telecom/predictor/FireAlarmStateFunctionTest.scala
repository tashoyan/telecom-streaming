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

  def heatEvent(timestampMillis: Long): Event =
    Event(new Timestamp(timestampMillis), siteId, severity, heatInfo)

  def smokeEvent(timestampMillis: Long): Event =
    Event(new Timestamp(timestampMillis), siteId, severity, smokeInfo)

  test("state exists [N] / state timed out [-] / heat [N] / smoke [N] / smoke-heat timeout [-]") {
    val groupState: GroupState[ProblemState] = mock[GroupState[ProblemState]]
    (groupState.hasTimedOut _)
      .expects()
      .atLeastOnce()
      .returns(false)
    (groupState.exists _)
      .expects()
      .atLeastOnce()
      .returns(false)

    val alarmStateFunction = new FireAlarmStateFunction(problemTimeoutMillis)

    val result = alarmStateFunction.updateAlarmState(siteId, Iterator.empty, groupState)
    assert(result.isEmpty, "Expected none alarms")

  }

}
