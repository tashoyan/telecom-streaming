package com.github.tashoyan.telecom.predictor

import com.github.tashoyan.telecom.event.FireAlarmUtil._
import com.github.tashoyan.telecom.event.{Alarm, Event}
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.conditions.Context
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.api.windowing.time.Time

import scala.collection.Map

class CepFirePredictor(
    override val problemTimeoutMillis: Long,
    eventOutOfOrdernessMillis: Long
) extends AbstractFlinkFirePredictor(eventOutOfOrdernessMillis) {

  override def predictAlarms(events: DataStream[Event]): DataStream[Alarm] = {
    val fireCandidates = events
      .filter(e => isFireCandidate(e))
      .assignTimestampsAndWatermarks(TimestampAssigner)
      .keyBy(_.siteId)

    val heatPattern = "heat"
    val smokePattern = "smoke"
    val pattern = Pattern.begin[Event](heatPattern, AfterMatchSkipStrategy.skipPastLastEvent()).where(_.isHeat).oneOrMore
      .next(smokePattern).where { (event: Event, ctx: Context[Event]) =>
        /*
        Smoke event should be strictly after heat event. Maybe minimal problem timeout is needed.
        CEP sorts events by timestamp in ascending order:
         https://ci.apache.org/projects/flink/flink-docs-release-1.7/dev/libs/cep.html#handling-lateness-in-event-time
        */
        val firstHeat = ctx.getEventsForPattern(heatPattern).head
        event.isSmoke && event.timestamp != firstHeat.timestamp
      }
      .within(Time.milliseconds(problemTimeoutMillis))

    val patternStream = CEP.pattern(fireCandidates, pattern)
    val alarms: DataStream[Alarm] = patternStream
      .select { foundPattern: Map[String, Iterable[Event]] =>
        val heat = foundPattern(heatPattern).head
        val smoke = foundPattern(smokePattern).head
        createFireAlarm(heat, smoke)
      }
    alarms
  }

}
