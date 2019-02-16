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

  protected def isHeatEvent(event: Event): Boolean =
    event.info != null &&
      event.info.toLowerCase.contains("heat")

  protected def isSmokeEvent(event: Event): Boolean =
    event.info != null &&
      event.info.toLowerCase.contains("smoke")

  protected def isInTriggerInterval(heat: Event, smoke: Event): Boolean =
    smoke.timestamp.getTime - heat.timestamp.getTime >= 0 &&
      smoke.timestamp.getTime - heat.timestamp.getTime <= problemTimeoutMillis

}

case class NoneState()(implicit problemTimeoutMillis: Long) extends AbstractFireAlarmState {

  override def transition(events: Iterator[Event]): FireAlarmState = {
    val (heatEvents, smokeEvents) =
      events.foldLeft((new mutable.TreeSet[Event](), new mutable.TreeSet[Event])) { case ((heats, smokes), event) =>
        if (isHeatEvent(event)) {
          (heats += event, smokes)
        } else if (isSmokeEvent(event)) {
          (heats, smokes += event)
        } else (heats, smokes)
      }

    if (heatEvents.isEmpty) {
      NoneState()
    } else if (smokeEvents.isEmpty) {
      HeatState(heatEvents.head)
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
    val smokeEvents = events.foldLeft(new mutable.TreeSet[Event]()) { case (smokes, event) =>
      if (isSmokeEvent(event)) {
        smokes += event
      } else {
        smokes
      }
    }

    smokeEvents.headOption
      .map { smokeEvent =>
        if (isInTriggerInterval(heatEvent, smokeEvent)) {
          HeatAndSmokeState(heatEvent, smokeEvent)
        } else {
          NoneState()
        }
      }
      .getOrElse(this)
  }

}

case class HeatAndSmokeState(heatEvent: Event, smokeEvent: Event) extends FireAlarmState {

  override def transition(events: Iterator[Event]): FireAlarmState = this

}
