package com.github.tashoyan.telecom.predictor

import com.github.tashoyan.telecom.event.FireAlarmUtil._
import com.github.tashoyan.telecom.event.{Alarm, Event}
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * Fire predictor implemented with
  * <a href="https://ci.apache.org/projects/flink/flink-docs-release-1.7/dev/stream/operators/joining.html#interval-join">interval join</a>.
  * <p>
  * A stream of heat events is joined with a stream of smoke events
  * with interval of (heat timestamp, heat timestamp + problem timeout),
  * excluding boundaries.
  * Within each time window, the first occurrence of (heat, smoke) events is taken as an alarm.
  * <p>
  * <b>Deduplication</b>
  * <p>
  * Deduplication occurs thanks to taking the first occurrence of each (heat, smoke) event pair.
  *
  * @param problemTimeoutMillis      Problem timeout interval in milliseconds.
  *                                  If a heat event is followed by a smoke event not later than after this timeout, then an alarm is generated.
  * @param eventOutOfOrdernessMillis Max time interval when events may come out of order.
  */
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

  object CreateAlarmFunction extends WindowFunction[(Event, Event), Alarm, Long, TimeWindow] {
    override def apply(siteId: Long, window: TimeWindow, windowCandidates: Iterable[(Event, Event)], out: Collector[Alarm]): Unit = {
      windowCandidates
        .toList
        .sortBy(_._1)
        .headOption
        .foreach {
          case (heat, smoke) =>
            val alarm = createFireAlarm(heat, smoke)
            out.collect(alarm)
        }
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
      .lowerBoundExclusive()
      .upperBoundExclusive()
      .process(JoinEventsFunction)

    val alarms: DataStream[Alarm] = alarmCandidates
      .keyBy(_._1.siteId)
      .timeWindow(Time.milliseconds(problemTimeoutMillis))
      .apply(CreateAlarmFunction)
    alarms
  }

}
