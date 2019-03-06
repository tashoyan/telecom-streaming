package com.github.tashoyan.telecom.predictor

import com.github.tashoyan.telecom.event.Event._
import com.github.tashoyan.telecom.event.{Alarm, Event}
import com.github.tashoyan.telecom.predictor.FirePredictor._
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
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

  private type SortedEvents = mutable.SortedSet[Event]

  private object AlarmAggregator extends AggregateFunction[Event, SortedEvents, Alarm] {
    private val problemTimeoutMillis0 = problemTimeoutMillis

    override def createAccumulator(): SortedEvents = new mutable.TreeSet[Event]()

    override def add(event: Event, acc: SortedEvents): SortedEvents = acc += event

    override def merge(acc1: SortedEvents, acc2: SortedEvents): SortedEvents = acc1 ++= acc2

    override def getResult(acc: SortedEvents): Alarm = {
      val smoke = acc.find(_.isSmoke)
        .getOrElse(
          throw new NoSuchElementException(s"No smoke event among ${acc.mkString(",")}." +
            " Incorrect implementation of the window trigger.")
        )
      val heat = acc.find(e => isInCausalRelationship(e, smoke, problemTimeoutMillis0) && e.isHeat)
        .getOrElse(
          throw new NoSuchElementException(s"No heat event within $problemTimeoutMillis0 milliseconds before $smoke among ${acc.mkString(",")}." +
            " Incorrect implementation of the session window.")
        )
      Alarm(
        smoke.timestamp,
        smoke.siteId,
        alarmSeverity,
        s"Fire on site ${smoke.siteId}. First heat at ${heat.timestamp}."
      )
    }
  }

  private val windowFunction: ProcessWindowFunction[Alarm, Alarm, Long, TimeWindow] = new ProcessWindowFunction[Alarm, Alarm, Long, TimeWindow] {
    override def process(siteId: Long, context: Context, alarms: Iterable[Alarm], out: Collector[Alarm]): Unit =
      out.collect(alarms.head)
  }

  private object OnSmokeTrigger extends Trigger[Event, TimeWindow] {
    private val delegate = EventTimeTrigger.create()

    override def onElement(element: Event, timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult =
      if (element.asInstanceOf[Event].isSmoke) {
        TriggerResult.FIRE
      } else
        delegate.onElement(element, timestamp, window, ctx)

    override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult =
      delegate.onProcessingTime(time, window, ctx)

    override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult =
      //TODO Or PURGE?
      TriggerResult.CONTINUE

    override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit =
      delegate.clear(window, ctx)

    override def canMerge: Boolean =
      delegate.canMerge

    override def onMerge(window: TimeWindow, ctx: Trigger.OnMergeContext): Unit =
      delegate.onMerge(window, ctx)
  }

  override def predictAlarms(events: DataStream[Event]): DataStream[Alarm] = {
    val alarms = events
      .filter(e => isFireCandidate(e))
      .assignTimestampsAndWatermarks(timestampAssigner)
      .keyBy(_.siteId)
      .window(EventTimeSessionWindows.withGap(Time.milliseconds(problemTimeoutMillis)))
      .trigger(OnSmokeTrigger)
      .aggregate(
        AlarmAggregator,
        windowFunction
      )
    alarms
  }

}
