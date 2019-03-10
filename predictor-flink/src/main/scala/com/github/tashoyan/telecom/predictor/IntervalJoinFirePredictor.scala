package com.github.tashoyan.telecom.predictor

import com.github.tashoyan.telecom.event.FireAlarmUtil._
import com.github.tashoyan.telecom.event.{Alarm, Event}
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

class IntervalJoinFirePredictor(
    override val problemTimeoutMillis: Long,
    eventOutOfOrdernessMillis: Long
) extends AbstractFlinkFirePredictor(eventOutOfOrdernessMillis) {

  object JoinEventsFunction extends ProcessJoinFunction[Event, Event, (Event, Event)] {
    override def processElement(
        heat: Event,
        smoke: Event,
        ctx: ProcessJoinFunction[Event, Event, (Event, Event)]#Context,
        out: Collector[(Event, Event)]
    ): Unit = {
      out.collect((heat, smoke))
    }
  }

  override def predictAlarms(events: DataStream[Event]): DataStream[Alarm] = {
    val fireCandidates = events
      .filter(e => isFireCandidate(e))

    val heats = fireCandidates
      .filter(_.isHeat)
      .assignTimestampsAndWatermarks(TimestampAssigner)
      .keyBy(_.siteId)
    val smokes = fireCandidates
      .filter(_.isSmoke)
      .assignTimestampsAndWatermarks(TimestampAssigner)
      .keyBy(_.siteId)

    val alarmCandidates: DataStream[(Event, Event)] = heats.intervalJoin(smokes)
      .between(Time.milliseconds(0L), Time.milliseconds(problemTimeoutMillis))
      //TODO Test for corner cases: heat and smoke have same timestamps
      .lowerBoundExclusive()
      .upperBoundExclusive()
      .process(JoinEventsFunction)

    val alarms: DataStream[Alarm] = alarmCandidates
      .keyBy(_._1.siteId)
      .timeWindow(Time.milliseconds(problemTimeoutMillis))
      .apply { (_, _, windowCandidates, out: Collector[Alarm]) =>
        windowCandidates.headOption.foreach { case (heat, smoke) =>
          val alarm = createFireAlarm(heat, smoke)
          out.collect(alarm)
        }
      }
    alarms
  }

}
