package com.github.tashoyan.telecom.predictor

import com.github.tashoyan.telecom.event.Event._
import com.github.tashoyan.telecom.event.{Alarm, Event}
import com.github.tashoyan.telecom.predictor.FirePredictor._
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SessionWindowTimeGapExtractor}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{EventTimeTrigger, Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable

class SessionWindowFirePredictor(
    override val problemTimeoutMillis: Long,
    eventOutOfOrdernessMillis: Long
) extends FirePredictor {

  private val timestampAssigner = new BoundedOutOfOrdernessTimestampExtractor[Event](Time.milliseconds(eventOutOfOrdernessMillis)) {
    override def extractTimestamp(event: Event): Long =
      event.timestamp.getTime
  }

  private object TimeGapExtractor extends SessionWindowTimeGapExtractor[Event] {
    private val problemTimeoutMillis0 = problemTimeoutMillis

    override def extract(event: Event): Long =
      if (event.isHeat)
        problemTimeoutMillis0
      else
        1L
  }

  private type SortedEvents = mutable.SortedSet[Event]

  private object AlarmAggregator extends AggregateFunction[Event, SortedEvents, Option[Alarm]] {
    private val problemTimeoutMillis0 = problemTimeoutMillis

    override def createAccumulator(): SortedEvents = new mutable.TreeSet[Event]()

    override def add(event: Event, acc: SortedEvents): SortedEvents = acc += event

    override def merge(acc1: SortedEvents, acc2: SortedEvents): SortedEvents = acc1 ++= acc2

    override def getResult(acc: SortedEvents): Option[Alarm] = {
      println(s"getResult: ${acc.mkString(",")}")
      for {
        smoke <- acc.find(_.isSmoke)
        heat <- acc.find(e => isInCausalRelationship(e, smoke, problemTimeoutMillis0) && e.isHeat)
      } yield Alarm(
        smoke.timestamp,
        smoke.siteId,
        alarmSeverity,
        s"Fire on site ${smoke.siteId}. First heat at ${heat.timestamp}."
      )
    }
  }

  private object WindowFunction extends ProcessWindowFunction[Option[Alarm], Alarm, Long, TimeWindow] {
    override def process(siteId: Long, context: Context, alarms: Iterable[Option[Alarm]], out: Collector[Alarm]): Unit =
      alarms.head.foreach(out.collect)
  }

  //TODO It's just a wrapper over EventTimeTrigger - remove it and use the wrapped trigger
  private object FireTrigger extends Trigger[Event, TimeWindow] {
    private val delegate = EventTimeTrigger.create()

    override def onElement(element: Event, timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
      val res = delegate.onElement(element, timestamp, window, ctx)
      println(s"onElement: $element, timestamp: $timestamp, window: $window; $res")
      res
    }

    override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
      val res = delegate.onProcessingTime(time, window, ctx)
      println(s"onProcessingTime: $time, window: $window; $res")
      res
    }

    override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
      val res = delegate.onEventTime(time, window, ctx)
      println(s"onEventTime: $time, window: $window; $res")
      res
    }

    override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
      delegate.clear(window, ctx)
      println(s"clear: $window")
    }

    override def canMerge: Boolean =
      delegate.canMerge

    override def onMerge(window: TimeWindow, ctx: Trigger.OnMergeContext): Unit = {
      delegate.onMerge(window, ctx)
      println(s"onMerge: $window")
    }
  }

  override def predictAlarms(events: DataStream[Event]): DataStream[Alarm] = {
    val alarms = events
      .filter(e => isFireCandidate(e))
      .assignTimestampsAndWatermarks(timestampAssigner)
      .keyBy(_.siteId)
      .window(EventTimeSessionWindows.withDynamicGap(TimeGapExtractor))
      .trigger(FireTrigger)
      .aggregate(
        AlarmAggregator,
        WindowFunction
      )
    alarms
  }

}
