package com.github.tashoyan.telecom.predictor

import com.github.tashoyan.telecom.event.FireAlarmUtil._
import com.github.tashoyan.telecom.event.{Alarm, Event}
import org.apache.flink.cep.EventComparator
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.api.windowing.time.Time

import scala.collection.Map

class CepFirePredictor(
    override val problemTimeoutMillis: Long,
    eventOutOfOrdernessMillis: Long
) extends AbstractFlinkFirePredictor(eventOutOfOrdernessMillis) {

  private object FireCandidateComparator extends EventComparator[Event] {
    override def compare(e1: Event, e2: Event): Int = {
      val byTimestamp = e1.timestamp compareTo e2.timestamp
      if (byTimestamp != 0)
        byTimestamp
      else
        /*
      For heat and smoke events having same timestamp:
      put the smoke event first, this disables pattern trigger.
      We count only smoke events strictly after heat events.
      */
        e1.isHeat compareTo e2.isHeat
    }
  }

  override def predictAlarms(events: DataStream[Event]): DataStream[Alarm] = {
    val fireCandidates = events
      .filter(e => isFireCandidate(e))
      .assignTimestampsAndWatermarks(TimestampAssigner)
      .keyBy(_.siteId)

    val heatPattern = "heat"
    val smokePattern = "smoke"
    val pattern = Pattern.begin[Event](heatPattern, AfterMatchSkipStrategy.skipPastLastEvent()).where(_.isHeat).oneOrMore
      .next(smokePattern).where(_.isSmoke)
      .within(Time.milliseconds(problemTimeoutMillis))

    val patternStream = CEP.pattern(fireCandidates, pattern, FireCandidateComparator)
    val alarms: DataStream[Alarm] = patternStream
      .select { foundPattern: Map[String, Iterable[Event]] =>
        val heat = foundPattern(heatPattern).head
        val smoke = foundPattern(smokePattern).head
        createFireAlarm(heat, smoke)
      }
    alarms
  }

}
