package com.github.tashoyan.telecom.predictor

import java.sql.Timestamp
import java.util.concurrent.TimeUnit

import com.github.tashoyan.telecom.event.Event
import com.github.tashoyan.telecom.util.Timestamps.RichTimestamp
import org.apache.spark.sql.streaming.GroupState
import org.scalamock.scalatest.MockFactory
import org.scalatest.FunSuite

class FireAlarmStateFunctionTest extends FunSuite with MockFactory {

  private val problemTimeoutMillis = TimeUnit.SECONDS.toMillis(10)

  private val siteId = 1L
  private val severity = "MAJOR"
  private val heatInfo = "Heat on site"
  private val smokeInfo = "Smoke on site"

  private def heatEvent(timestamp: Timestamp): Event =
    Event(timestamp.getTime, siteId, severity, heatInfo)

  private def smokeEvent(timestamp: Timestamp): Event =
    Event(timestamp.getTime, siteId, severity, smokeInfo)

  private def heatProblemState(timestamp: Timestamp): ProblemState =
    ProblemState(heatEvent(timestamp))

  /* Non-existing state */

  test("state exists [N] / state timed out [-] / heat [N] / smoke [N] / smoke-heat timeout [-]") {
    val events = Iterator.empty

    val state: GroupState[ProblemState] = mock[GroupState[ProblemState]]
    (state.exists _)
      .expects()
      .atLeastOnce()
      .returns(false)

    val alarmStateFunction = new FireAlarmStateFunction(problemTimeoutMillis)

    val alarms = alarmStateFunction.updateAlarmState(siteId, events, state)
    assert(alarms.isEmpty, "Expected none alarms")
  }

  test("state exists [N] / state timed out [-] / heat [Y] / smoke [N] / smoke-heat timeout [-]") {
    val heatTimestamp = new Timestamp(1000L)
    val events = Iterator(heatEvent(heatTimestamp))

    val state: GroupState[ProblemState] = mock[GroupState[ProblemState]]
    inSequence {
      (state.exists _)
        .expects()
        .atLeastOnce()
        .returns(false)
      inAnyOrder {
        (state.update _)
          .expects(heatProblemState(heatTimestamp))
          .once()
        (state.setTimeoutTimestamp(_: Long))
          .expects(heatTimestamp.getTime + problemTimeoutMillis)
          .once()
      }
    }

    val alarmStateFunction = new FireAlarmStateFunction(problemTimeoutMillis)

    val alarms = alarmStateFunction.updateAlarmState(siteId, events, state)
    assert(alarms.isEmpty, "Expected none alarms")
  }

  test("state exists [N] / state timed out [-] / heat [N] / smoke [Y] / smoke-heat timeout [-]") {
    val smokeTimestamp = new Timestamp(1000L)
    val events = Iterator(smokeEvent(smokeTimestamp))

    val state: GroupState[ProblemState] = mock[GroupState[ProblemState]]
    (state.exists _)
      .expects()
      .atLeastOnce()
      .returns(false)

    val alarmStateFunction = new FireAlarmStateFunction(problemTimeoutMillis)

    val alarms = alarmStateFunction.updateAlarmState(siteId, events, state)
    assert(alarms.isEmpty, "Expected none alarms")
  }

  test("state exists [N] / state timed out [-] / heat [Y] / smoke [Y] / smoke-heat timeout [N]") {
    val heatTimestamp = new Timestamp(1000L)
    val smokeTimestamp = new Timestamp(heatTimestamp.getTime + problemTimeoutMillis / 2)
    val events = Iterator(heatEvent(heatTimestamp), smokeEvent(smokeTimestamp))

    val state: GroupState[ProblemState] = mock[GroupState[ProblemState]]
    (state.exists _)
      .expects()
      .atLeastOnce()
      .returns(false)
    val alarmStateFunction = new FireAlarmStateFunction(problemTimeoutMillis)

    val alarms = alarmStateFunction.updateAlarmState(siteId, events, state).toSeq
    assert(alarms.length === 1, "Expected 1 alarm")
    val alarm = alarms.head
    assert(alarm.timestamp === smokeTimestamp)
    assert(alarm.objectId === siteId)
    assert(alarm.info.toLowerCase.contains("fire "))
  }

  test("state exists [N] / state timed out [-] / heat [Y] / smoke [Y] / smoke-heat timeout [Y]") {
    val heatTimestamp = new Timestamp(1000L)
    val smokeTimestamp = new Timestamp(heatTimestamp.getTime + problemTimeoutMillis * 2)
    val events = Iterator(heatEvent(heatTimestamp), smokeEvent(smokeTimestamp))

    val state: GroupState[ProblemState] = mock[GroupState[ProblemState]]
    (state.exists _)
      .expects()
      .atLeastOnce()
      .returns(false)
    val alarmStateFunction = new FireAlarmStateFunction(problemTimeoutMillis)

    val alarms = alarmStateFunction.updateAlarmState(siteId, events, state).toSeq
    assert(alarms.isEmpty, "Expected none alarms")
  }

  /* Existing state, not timed out */

  test("state exists [Y] / state timed out [N] / heat [N] / smoke [N] / smoke-heat timeout [-]") {
    val events = Iterator.empty

    val heat0Timestamp = new Timestamp(1000L)
    val state: GroupState[ProblemState] = mock[GroupState[ProblemState]]
    inSequence {
      inAnyOrder {
        (state.exists _)
          .expects()
          .atLeastOnce()
          .returns(true)
        (state.hasTimedOut _)
          .expects()
          .atLeastOnce()
          .returns(false)
      }
      (state.get _)
        .expects()
        .atLeastOnce()
        .returns(heatProblemState(heat0Timestamp))
      inAnyOrder {
        (state.update _)
          .expects(heatProblemState(heat0Timestamp))
          .once()
        (state.setTimeoutTimestamp(_: Long))
          .expects(heat0Timestamp.getTime + problemTimeoutMillis)
          .once()
      }
    }

    val alarmStateFunction = new FireAlarmStateFunction(problemTimeoutMillis)

    val alarms = alarmStateFunction.updateAlarmState(siteId, events, state)
    assert(alarms.isEmpty, "Expected none alarms")
  }

  test("state exists [Y] / state timed out [N] / heat [Y] / smoke [N] / smoke-heat timeout [-]") {
    val heat0Timestamp = new Timestamp(1000L)
    val heatTimestamp = new Timestamp(heat0Timestamp.getTime + problemTimeoutMillis / 2)
    val events = Iterator(heatEvent(heatTimestamp))

    val state: GroupState[ProblemState] = mock[GroupState[ProblemState]]
    inSequence {
      inAnyOrder {
        (state.exists _)
          .expects()
          .atLeastOnce()
          .returns(true)
        (state.hasTimedOut _)
          .expects()
          .atLeastOnce()
          .returns(false)
      }
      (state.get _)
        .expects()
        .atLeastOnce()
        .returns(heatProblemState(heat0Timestamp))
      inAnyOrder {
        (state.update _)
          .expects(heatProblemState(heatTimestamp))
          .once()
        (state.setTimeoutTimestamp(_: Long))
          .expects(heatTimestamp.getTime + problemTimeoutMillis)
          .once()
      }
    }

    val alarmStateFunction = new FireAlarmStateFunction(problemTimeoutMillis)

    val alarms = alarmStateFunction.updateAlarmState(siteId, events, state)
    assert(alarms.isEmpty, "Expected none alarms")
  }

  test("state exists [Y] / state timed out [N] / heat [N] / smoke [Y] / smoke-heat timeout [N]") {
    val heat0Timestamp = new Timestamp(1000L)
    val smokeTimestamp = new Timestamp(heat0Timestamp.getTime + problemTimeoutMillis / 2)
    val events = Iterator(smokeEvent(smokeTimestamp))

    val state: GroupState[ProblemState] = mock[GroupState[ProblemState]]
    inSequence {
      inAnyOrder {
        (state.exists _)
          .expects()
          .atLeastOnce()
          .returns(true)
        (state.hasTimedOut _)
          .expects()
          .atLeastOnce()
          .returns(false)
      }
      (state.get _)
        .expects()
        .atLeastOnce()
        .returns(heatProblemState(heat0Timestamp))
      (state.remove _)
        .expects()
        .once()
    }

    val alarmStateFunction = new FireAlarmStateFunction(problemTimeoutMillis)

    val alarms = alarmStateFunction.updateAlarmState(siteId, events, state).toSeq
    assert(alarms.length === 1, "Expected 1 alarm")
    val alarm = alarms.head
    assert(alarm.timestamp === smokeTimestamp)
    assert(alarm.objectId === siteId)
    assert(alarm.info.toLowerCase.contains("fire "))
  }

  test("state exists [Y] / state timed out [N] / heat [N] / smoke [Y] / smoke-heat timeout [Y]") {
    val heat0Timestamp = new Timestamp(1000L)
    val smokeTimestamp = new Timestamp(heat0Timestamp.getTime + problemTimeoutMillis * 2)
    val events = Iterator(smokeEvent(smokeTimestamp))

    val state: GroupState[ProblemState] = mock[GroupState[ProblemState]]
    inSequence {
      inAnyOrder {
        (state.exists _)
          .expects()
          .atLeastOnce()
          .returns(true)
        (state.hasTimedOut _)
          .expects()
          .atLeastOnce()
          .returns(false)
      }
      (state.get _)
        .expects()
        .atLeastOnce()
        .returns(heatProblemState(heat0Timestamp))
      (state.remove _)
        .expects()
        .once()
    }

    val alarmStateFunction = new FireAlarmStateFunction(problemTimeoutMillis)

    val alarms = alarmStateFunction.updateAlarmState(siteId, events, state)
    assert(alarms.isEmpty, "Expected none alarms")
  }

  test("state exists [Y] / state timed out [N] / heat [Y] / smoke [Y] / smoke-heat timeout [N]") {
    val heat0Timestamp = new Timestamp(1000L)
    val heatTimestamp = new Timestamp(heat0Timestamp.getTime + problemTimeoutMillis / 2)
    val smokeTimestamp = new Timestamp(heat0Timestamp.getTime + problemTimeoutMillis / 2)
    val events = Iterator(heatEvent(heatTimestamp), smokeEvent(smokeTimestamp))

    val state: GroupState[ProblemState] = mock[GroupState[ProblemState]]
    inSequence {
      inAnyOrder {
        (state.exists _)
          .expects()
          .atLeastOnce()
          .returns(true)
        (state.hasTimedOut _)
          .expects()
          .atLeastOnce()
          .returns(false)
      }
      (state.get _)
        .expects()
        .atLeastOnce()
        .returns(heatProblemState(heat0Timestamp))
      (state.remove _)
        .expects()
        .once()
    }

    val alarmStateFunction = new FireAlarmStateFunction(problemTimeoutMillis)

    val alarms = alarmStateFunction.updateAlarmState(siteId, events, state).toSeq
    assert(alarms.length === 1, "Expected 1 alarm")
    val alarm = alarms.head
    assert(alarm.timestamp === smokeTimestamp)
    assert(alarm.objectId === siteId)
    assert(alarm.info.toLowerCase.contains("fire "))
  }

  test("state exists [Y] / state timed out [N] / heat [Y] / smoke [Y] / smoke-heat timeout [Y]") {
    val heat0Timestamp = new Timestamp(1000L)
    val heatTimestamp = new Timestamp(heat0Timestamp.getTime + problemTimeoutMillis / 2)
    val smokeTimestamp = new Timestamp(heat0Timestamp.getTime + problemTimeoutMillis * 2)
    val events = Iterator(heatEvent(heatTimestamp), smokeEvent(smokeTimestamp))

    val state: GroupState[ProblemState] = mock[GroupState[ProblemState]]
    inSequence {
      inAnyOrder {
        (state.exists _)
          .expects()
          .atLeastOnce()
          .returns(true)
        (state.hasTimedOut _)
          .expects()
          .atLeastOnce()
          .returns(false)
      }
      (state.get _)
        .expects()
        .atLeastOnce()
        .returns(heatProblemState(heat0Timestamp))
      (state.remove _)
        .expects()
        .once()
    }

    val alarmStateFunction = new FireAlarmStateFunction(problemTimeoutMillis)

    val alarms = alarmStateFunction.updateAlarmState(siteId, events, state)
    assert(alarms.isEmpty, "Expected none alarms")
  }

  /* Existing state, timed out */

  test("state exists [Y] / state timed out [Y] / heat [N] / smoke [N] / smoke-heat timeout [-]") {
    val events = Iterator.empty

    val state: GroupState[ProblemState] = mock[GroupState[ProblemState]]
    inSequence {
      inAnyOrder {
        (state.exists _)
          .expects()
          .atLeastOnce()
          .returns(true)
        (state.hasTimedOut _)
          .expects()
          .atLeastOnce()
          .returns(true)
      }
      (state.remove _)
        .expects()
        .once()
    }

    val alarmStateFunction = new FireAlarmStateFunction(problemTimeoutMillis)

    val alarms = alarmStateFunction.updateAlarmState(siteId, events, state)
    assert(alarms.isEmpty, "Expected none alarms")
  }

  test("state exists [Y] / state timed out [Y] / heat [Y] / smoke [N] / smoke-heat timeout [-]") {
    val heatTimestamp = new Timestamp(1000L)
    val events = Iterator(heatEvent(heatTimestamp))

    val state: GroupState[ProblemState] = mock[GroupState[ProblemState]]
    inSequence {
      inAnyOrder {
        (state.exists _)
          .expects()
          .atLeastOnce()
          .returns(true)
        (state.hasTimedOut _)
          .expects()
          .atLeastOnce()
          .returns(true)
      }
      (state.remove _)
        .expects()
        .once()
    }

    val alarmStateFunction = new FireAlarmStateFunction(problemTimeoutMillis)

    val alarms = alarmStateFunction.updateAlarmState(siteId, events, state)
    assert(alarms.isEmpty, "Expected none alarms")
  }

  test("state exists [Y] / state timed out [Y] / heat [N] / smoke [Y] / smoke-heat timeout [N|Y]") {
    val smokeTimestamp = new Timestamp(1000L)
    val events = Iterator(smokeEvent(smokeTimestamp))

    val state: GroupState[ProblemState] = mock[GroupState[ProblemState]]
    inSequence {
      inAnyOrder {
        (state.exists _)
          .expects()
          .atLeastOnce()
          .returns(true)
        (state.hasTimedOut _)
          .expects()
          .atLeastOnce()
          .returns(true)
      }
      (state.remove _)
        .expects()
        .once()
    }

    val alarmStateFunction = new FireAlarmStateFunction(problemTimeoutMillis)

    val alarms = alarmStateFunction.updateAlarmState(siteId, events, state)
    assert(alarms.isEmpty, "Expected none alarms")
  }

  test("state exists [Y] / state timed out [Y] / heat [Y] / smoke [Y] / smoke-heat timeout [N|Y]") {
    val heatTimestamp = new Timestamp(1000L)
    val smokeTimestamp = new Timestamp(2000L)
    val events = Iterator(heatEvent(heatTimestamp), smokeEvent(smokeTimestamp))

    val state: GroupState[ProblemState] = mock[GroupState[ProblemState]]
    inSequence {
      inAnyOrder {
        (state.exists _)
          .expects()
          .atLeastOnce()
          .returns(true)
        (state.hasTimedOut _)
          .expects()
          .atLeastOnce()
          .returns(true)
      }
      (state.remove _)
        .expects()
        .once()
    }

    val alarmStateFunction = new FireAlarmStateFunction(problemTimeoutMillis)

    val alarms = alarmStateFunction.updateAlarmState(siteId, events, state)
    assert(alarms.isEmpty, "Expected none alarms")
  }

  /* Multiple events in a batch */

  test("state exists [N] / state timed out [-] / heat [Y] multiple / smoke [Y] multiple / smoke-heat timeout [N]") {
    val heatTimestamps = Seq(3, 1, 5)
      .map(i => i * problemTimeoutMillis / 10)
      .map(millis => new Timestamp(millis))
    val smokeTimestamps = Seq(20, 14, 25)
      .map(i => i * problemTimeoutMillis / 10)
      .map(millis => new Timestamp(millis))
    val events = (heatTimestamps.map(heatEvent) ++ smokeTimestamps.map(smokeEvent))
      .toIterator

    val state: GroupState[ProblemState] = mock[GroupState[ProblemState]]
    (state.exists _)
      .expects()
      .atLeastOnce()
      .returns(false)

    val alarmStateFunction = new FireAlarmStateFunction(problemTimeoutMillis)

    val alarms = alarmStateFunction.updateAlarmState(siteId, events, state).toSeq
    assert(alarms.length === 1, "Expected 1 alarm")
    val alarm = alarms.head
    assert(alarm.timestamp === smokeTimestamps.min)
    assert(alarm.objectId === siteId)
    assert(alarm.info.toLowerCase.contains("fire "))
  }

  test("state exists [N] / state timed out [-] / heat [Y] multiple / smoke [Y] multiple / smoke-heat timeout [Y]") {
    val heatTimestamps = Seq(3, 1, 5)
      .map(i => i * problemTimeoutMillis / 10)
      .map(millis => new Timestamp(millis))
    val smokeTimestamps = Seq(20, 16, 25)
      .map(i => i * problemTimeoutMillis / 10)
      .map(millis => new Timestamp(millis))
    val events = (heatTimestamps.map(heatEvent) ++ smokeTimestamps.map(smokeEvent))
      .toIterator

    val state: GroupState[ProblemState] = mock[GroupState[ProblemState]]
    (state.exists _)
      .expects()
      .atLeastOnce()
      .returns(false)

    val alarmStateFunction = new FireAlarmStateFunction(problemTimeoutMillis)

    val alarms = alarmStateFunction.updateAlarmState(siteId, events, state)
    assert(alarms.isEmpty, "Expected none alarms")
  }

  test("state exists [Y] / state timed out [N] / heat [Y] multiple / smoke [Y] multiple / smoke-heat timeout [N]") {
    val heat0Timestamp = new Timestamp(1000L)

    val heatTimestamps = Seq(3, 1, 5)
      .map(i => heat0Timestamp.getTime + i * problemTimeoutMillis / 10)
      .map(millis => new Timestamp(millis))
    val smokeTimestamps = Seq(20, 14, 25)
      .map(i => heat0Timestamp.getTime + i * problemTimeoutMillis / 10)
      .map(millis => new Timestamp(millis))
    val events = (heatTimestamps.map(heatEvent) ++ smokeTimestamps.map(smokeEvent))
      .toIterator

    val state: GroupState[ProblemState] = mock[GroupState[ProblemState]]
    inSequence {
      inAnyOrder {
        (state.exists _)
          .expects()
          .atLeastOnce()
          .returns(true)
        (state.hasTimedOut _)
          .expects()
          .atLeastOnce()
          .returns(false)
      }
      (state.get _)
        .expects()
        .atLeastOnce()
        .returns(heatProblemState(heat0Timestamp))
      (state.remove _)
        .expects()
        .once()
    }

    val alarmStateFunction = new FireAlarmStateFunction(problemTimeoutMillis)

    val alarms = alarmStateFunction.updateAlarmState(siteId, events, state).toSeq
    assert(alarms.length === 1, "Expected 1 alarm")
    val alarm = alarms.head
    assert(alarm.timestamp === smokeTimestamps.min)
    assert(alarm.objectId === siteId)
    assert(alarm.info.toLowerCase.contains("fire "))
  }

  test("state exists [Y] / state timed out [N] / heat [Y] multiple / smoke [Y] multiple / smoke-heat timeout [Y]") {
    val heat0Timestamp = new Timestamp(1000L)
    val heatTimestamps = Seq(3, 1, 5)
      .map(i => i * problemTimeoutMillis / 10)
      .map(millis => new Timestamp(millis))
    val smokeTimestamps = Seq(20, 16, 25)
      .map(i => i * problemTimeoutMillis / 10)
      .map(millis => new Timestamp(millis))
    val events = (heatTimestamps.map(heatEvent) ++ smokeTimestamps.map(smokeEvent))
      .toIterator

    val state: GroupState[ProblemState] = mock[GroupState[ProblemState]]
    inSequence {
      inAnyOrder {
        (state.exists _)
          .expects()
          .atLeastOnce()
          .returns(true)
        (state.hasTimedOut _)
          .expects()
          .atLeastOnce()
          .returns(false)
      }
      (state.get _)
        .expects()
        .atLeastOnce()
        .returns(heatProblemState(heat0Timestamp))
      (state.remove _)
        .expects()
        .once()
    }

    val alarmStateFunction = new FireAlarmStateFunction(problemTimeoutMillis)

    val alarms = alarmStateFunction.updateAlarmState(siteId, events, state)
    assert(alarms.isEmpty, "Expected none alarms")
  }

}
