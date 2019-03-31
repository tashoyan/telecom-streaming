package com.github.tashoyan.telecom.predictor

import com.github.tashoyan.telecom.event.Event._
import com.github.tashoyan.telecom.event.FireAlarmUtil._
import com.github.tashoyan.telecom.event.{Alarm, Event}

import scala.annotation.tailrec
import scala.collection.{SortedSet, mutable}

//TODO Use for Spark Predictor as well
//TODO Test
class FireAlarmState private (events: mutable.SortedSet[Event])(implicit problemTimeoutMillis: Long) {

  def this()(implicit problemTimeoutMillis: Long) =
    this(mutable.TreeSet.empty[Event])

  def update(inputEvents: Iterator[Event]): FireAlarmState = {
    val updatedEvents = events ++= inputEvents
    new FireAlarmState(updatedEvents)
  }

  def getFireAlarm: Option[Alarm] = {
    findAlarmSequence(events)
      .map { case (heatEvent, smokeEvent) => createFireAlarm(heatEvent, smokeEvent) }
  }

  private def findAlarmSequence(events: SortedSet[Event]): Option[(Event, Event)] = {
    val smokeEvents = events.toStream.filter(_.isSmoke)
    findAlarmSequence(events, smokeEvents)
  }

  @tailrec private def findAlarmSequence(events: SortedSet[Event], smokeEvents: Stream[Event]): Option[(Event, Event)] = {
    if (smokeEvents.isEmpty)
      None
    else {
      val alarmSequence = findAlarmSequence(events, smokeEvents.head)
      if (alarmSequence.isDefined)
        alarmSequence
      else
        findAlarmSequence(events, smokeEvents.tail)
    }
  }

  private def findAlarmSequence(events: SortedSet[Event], smokeEvent: Event): Option[(Event, Event)] = {
    events
      .until(smokeEvent)
      .filter(_.isHeat)
      .find(heatEvent => isInCausalRelationship(heatEvent, smokeEvent, problemTimeoutMillis - 1L))
      .map(heatEvent => (heatEvent, smokeEvent))
  }

  override def toString: String =
    s"${getClass.getSimpleName}($events)"

}
