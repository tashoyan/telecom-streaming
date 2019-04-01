package com.github.tashoyan.telecom.predictor

import com.github.tashoyan.telecom.event.{Event, FireAlarmState}

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
