package com.github.tashoyan.telecom.predictor

import com.github.tashoyan.telecom.spark.SparkEvent
import com.github.tashoyan.telecom.spark.SparkEvent._

import scala.collection.mutable

//TODO Tests
trait FireAlarmState {

  def transition(events: Iterator[SparkEvent]): FireAlarmState

}

abstract class AbstractFireAlarmState extends FireAlarmState {

  protected def findImportantSortedEvents(events: Iterator[SparkEvent]): (mutable.SortedSet[SparkEvent], mutable.SortedSet[SparkEvent]) = {
    events.foldLeft((new mutable.TreeSet[SparkEvent](), new mutable.TreeSet[SparkEvent])) { case ((heats, smokes), event) =>
      if (event.isHeat) {
        (heats += event, smokes)
      } else if (event.isSmoke) {
        (heats, smokes += event)
      } else (heats, smokes)
    }
  }

}

case class NoneState()(implicit problemTimeoutMillis: Long) extends AbstractFireAlarmState {

  override def transition(events: Iterator[SparkEvent]): FireAlarmState = {
    val (heatEvents, smokeEvents) = findImportantSortedEvents(events)

    if (heatEvents.isEmpty) {
      this
    } else if (smokeEvents.isEmpty) {
      HeatState(heatEvents.last)
    } else {
      val smokeEvent = smokeEvents.head
      heatEvents.find(heatEvent => isInCausalRelationship(heatEvent, smokeEvent, problemTimeoutMillis))
        .map(heatEvent => HeatAndSmokeState(heatEvent, smokeEvent))
        .getOrElse(this)
    }
  }

}

case class HeatState(heatEvent: SparkEvent)(implicit problemTimeoutMillis: Long) extends AbstractFireAlarmState {

  override def transition(events: Iterator[SparkEvent]): FireAlarmState = {
    val (heatEvents, smokeEvents) = findImportantSortedEvents(events)

    if (smokeEvents.nonEmpty) {
      val smokeEvent = smokeEvents.head
      val allHeatEvents = heatEvents + heatEvent
      allHeatEvents.find(heat => isInCausalRelationship(heat, smokeEvent, problemTimeoutMillis))
        .map(heat => HeatAndSmokeState(heat, smokeEvent))
        .getOrElse(NoneState())
    } else {
      heatEvents.lastOption
        .map(heat => HeatState(heat))
        .getOrElse(this)
    }
  }

}

case class HeatAndSmokeState(heatEvent: SparkEvent, smokeEvent: SparkEvent) extends FireAlarmState {

  override def transition(events: Iterator[SparkEvent]): FireAlarmState = this

}
