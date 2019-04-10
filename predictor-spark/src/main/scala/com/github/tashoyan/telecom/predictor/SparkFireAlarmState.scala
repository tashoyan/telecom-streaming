package com.github.tashoyan.telecom.predictor

import com.github.tashoyan.telecom.event.{Event, FireAlarmState}

/**
  * A wrapper over [[FireAlarmState]] encodable by Spark SQL.
  * We need it to use the alarm state with Spark GroupState API.
  *
  * @param heatEvents           Heat events in the current state, in the chronological order.
  * @param smokeEvents          Smoke events in the current state, in the chronological order.
  * @param problemTimeoutMillis Problem timeout in milliseconds.
  */
case class SparkFireAlarmState(heatEvents: Seq[Event], smokeEvents: Seq[Event], problemTimeoutMillis: Long) {

  def toFireAlarmState: FireAlarmState =
    new FireAlarmState()(problemTimeoutMillis)
      .update(heatEvents.iterator)
      .update(smokeEvents.iterator)

}

object SparkFireAlarmState {

  def apply(fireAlarmState: FireAlarmState): SparkFireAlarmState =
    new SparkFireAlarmState(fireAlarmState.getHeatEvents, fireAlarmState.getSmokeEvents, fireAlarmState.problemTimeoutMillis)

}
