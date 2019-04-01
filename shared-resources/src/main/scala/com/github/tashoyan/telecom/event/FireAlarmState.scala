package com.github.tashoyan.telecom.event

import com.github.tashoyan.telecom.event.Event._
import com.github.tashoyan.telecom.event.FireAlarmState._
import com.github.tashoyan.telecom.event.FireAlarmUtil._

import scala.annotation.tailrec
import scala.collection.mutable

//TODO Use for Spark Predictor as well
//TODO Test
class FireAlarmState private (heatEvents: mutable.SortedSet[Event], smokeEvents: mutable.SortedSet[Event])(implicit val problemTimeoutMillis: Long) {

  def this()(implicit problemTimeoutMillis: Long) =
    this(new mutable.TreeSet[Event], new mutable.TreeSet[Event])

  def update(inputEvents: Iterator[Event]): FireAlarmState = {
    val (inputHeats, inputSmokes) = extractHeatSmokeEvents(inputEvents)
    new FireAlarmState(
      heatEvents ++= inputHeats,
      smokeEvents ++= inputSmokes
    )
  }

  def getFireAlarm: Option[Alarm] = {
    findAlarmSequence(smokeEvents.toStream)
      .map { case (heatEvent, smokeEvent) => createFireAlarm(heatEvent, smokeEvent) }
  }

  @tailrec private def findAlarmSequence(smokeEventStream: Stream[Event]): Option[(Event, Event)] = {
    if (smokeEventStream.isEmpty)
      None
    else {
      val alarmSequence = findAlarmSequence(smokeEventStream.head)
      if (alarmSequence.isDefined)
        alarmSequence
      else
        findAlarmSequence(smokeEventStream.tail)
    }
  }

  private def findAlarmSequence(smokeEvent: Event): Option[(Event, Event)] = {
    heatEvents
      .until(smokeEvent)
      .find(heatEvent => isInCausalRelationship(heatEvent, smokeEvent, problemTimeoutMillis - 1L))
      .map(heatEvent => (heatEvent, smokeEvent))
  }

  def maxTimestamp: Option[Long] = {
    val maxHeatTimestamp = heatEvents.lastOption.map(_.timestamp)
    val maxSmokeTimestamp = smokeEvents.lastOption.map(_.timestamp)
    val maxTs = for {
      heatTs <- maxHeatTimestamp
      smokeTs <- maxSmokeTimestamp
    } yield math.max(heatTs, smokeTs)
    maxTs
      .orElse(maxHeatTimestamp)
      .orElse(maxSmokeTimestamp)
  }

  def getHeatEvents: Seq[Event] =
    heatEvents.toSeq

  def getSmokeEvents: Seq[Event] =
    smokeEvents.toSeq

  override def toString: String =
    s"${getClass.getSimpleName}($heatEvents, $smokeEvents)"

}

object FireAlarmState {

  private def extractHeatSmokeEvents(events: Iterator[Event]): (mutable.SortedSet[Event], mutable.SortedSet[Event]) = {
    events.foldLeft((new mutable.TreeSet[Event], new mutable.TreeSet[Event])) { case ((heats, smokes), event) =>
      if (event.isHeat) {
        (heats += event, smokes)
      } else if (event.isSmoke) {
        (heats, smokes += event)
      } else (heats, smokes)
    }
  }

}
