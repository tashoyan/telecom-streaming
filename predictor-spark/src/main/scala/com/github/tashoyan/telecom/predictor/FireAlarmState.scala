package com.github.tashoyan.telecom.predictor

import com.github.tashoyan.telecom.event.Event
import com.github.tashoyan.telecom.util.Timestamps.RichTimestamp

import scala.collection.mutable

//TODO Tests
trait FireAlarmState {

  def transition(events: Iterator[Event]): FireAlarmState

}

abstract class AbstractFireAlarmState(implicit problemTimeoutMillis: Long) extends FireAlarmState {

  protected implicit val eventOrdering: Ordering[Event] =
    Ordering.by(_.timestamp)

  protected def findImportantSortedEvents(events: Iterator[Event]): (mutable.TreeSet[Event], mutable.TreeSet[Event]) = {
    events.foldLeft((new mutable.TreeSet[Event](), new mutable.TreeSet[Event])) { case ((heats, smokes), event) =>
      if (event.isHeat) {
        (heats += event, smokes)
      } else if (event.isSmoke) {
        (heats, smokes += event)
      } else (heats, smokes)
    }
  }

  protected def isInTriggerInterval(heat: Event, smoke: Event): Boolean =
    smoke.timestamp.getTime - heat.timestamp.getTime >= 0 &&
      smoke.timestamp.getTime - heat.timestamp.getTime <= problemTimeoutMillis

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
      heatEvents.find(heatEvent => isInTriggerInterval(heatEvent, smokeEvent))
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
      allHeatEvents.find(heat => isInTriggerInterval(heat, smokeEvent))
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
