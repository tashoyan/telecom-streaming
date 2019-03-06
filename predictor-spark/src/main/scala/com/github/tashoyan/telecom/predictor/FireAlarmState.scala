package com.github.tashoyan.telecom.predictor

import com.github.tashoyan.telecom.event.Event
import com.github.tashoyan.telecom.event.Event._

import scala.collection.mutable

//TODO Tests
trait FireAlarmState {

  def transition(events: Iterator[Event]): FireAlarmState

}

abstract class AbstractFireAlarmState extends FireAlarmState {

  protected def findImportantSortedEvents(events: Iterator[Event]): (mutable.SortedSet[Event], mutable.SortedSet[Event]) = {
    events.foldLeft((new mutable.TreeSet[Event](), new mutable.TreeSet[Event])) { case ((heats, smokes), event) =>
      if (event.isHeat) {
        (heats += event, smokes)
      } else if (event.isSmoke) {
        (heats, smokes += event)
      } else (heats, smokes)
    }
  }

}

case class NoneState()(implicit problemTimeoutMillis: Long) extends AbstractFireAlarmState {

  override def transition(events: Iterator[Event]): FireAlarmState = {
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

case class HeatState(heatEvent: Event)(implicit problemTimeoutMillis: Long) extends AbstractFireAlarmState {

  override def transition(events: Iterator[Event]): FireAlarmState = {
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

case class HeatAndSmokeState(heatEvent: Event, smokeEvent: Event) extends FireAlarmState {

  override def transition(events: Iterator[Event]): FireAlarmState = this

}
