package com.github.tashoyan.telecom.predictor

import java.sql.Timestamp

import com.github.tashoyan.telecom.predictor.FlinkSocketWindowWordCount.TimestampFormat.formatTime
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.extensions._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{EventTimeTrigger, Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/*
$ nc -lk 9999
$ mvn -DskipTests -Dskip -pl :predictor-flink install && mvn exec:java -pl :predictor-flink -Dexec.mainClass=com.github.tashoyan.telecom.predictor.FlinkSocketSessionPredictor -Dexec.args="--port 9999"
*/
//scalastyle:off
object FlinkSocketSessionPredictor {
  private val sessionTimeoutSec = 5L
  private val watermarkSec = 5L
  private val watermarkCheckIntervalMillis = 1000L

  def main(args: Array[String]): Unit = {
    val port: Int = try {
      ParameterTool.fromArgs(args).getInt("port")
    } catch {
      case e: Exception =>
        throw new RuntimeException(s"Cannot parse the port from args: '${args.mkString(",")}'", e)
    }

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(watermarkCheckIntervalMillis)

    val timestampText: DataStream[(String, Timestamp)] = env.socketTextStream("localhost", port, '\n')
      .filter(_.nonEmpty)
      .map((_, new Timestamp(System.currentTimeMillis())))

    val events: DataStream[Event] = timestampText.flatMapWith { case (text, timestamp) =>
      text
        .split("\\s+")
        .map(Event(timestamp, _))
    }
    events.print()
      .setParallelism(1)

    val wmAssigner = new BoundedOutOfOrdernessTimestampExtractor[Event](Time.seconds(watermarkSec)) {
      override def extractTimestamp(event: Event): Long = event.timestamp.getTime
    }

    val preAggregator = new AggregateFunction[Event, Long, Long] {
      override def createAccumulator(): Long = Long.MinValue

      override def add(event: Event, acc: Long): Long = math.max(event.timestamp.getTime, acc)

      override def merge(acc1: Long, acc2: Long): Long = math.max(acc1, acc2)

      override def getResult(acc: Long): Long = acc
    }

    val windowFunction: ProcessWindowFunction[Long, Alarm, String, TimeWindow] = new ProcessWindowFunction[Long, Alarm, String, TimeWindow] {
      override def process(key: String, context: Context, elements: Iterable[Long], out: Collector[Alarm]): Unit = {
        val alarmTimestamp = new Timestamp(elements.head)
        val alarm = Alarm(
          alarmTimestamp,
          s"Site: $key, last event timestamp: ${formatTime(new Timestamp(elements.head))}"
        )
        out.collect(alarm)
      }
    }

    val alarms = events
      .assignTimestampsAndWatermarks(wmAssigner)
      //Assume all events come from the same site
      .keyBy(_ => "site1")
      .window(EventTimeSessionWindows.withGap(Time.seconds(sessionTimeoutSec)))
      .trigger(new CustomTrigger())
      .aggregate(
        preAggregator,
        windowFunction
      )

    alarms.print()
      .setParallelism(1)

    env.execute(this.getClass.getSimpleName)
    ()
  }

  case class Event(timestamp: Timestamp, eventType: String) {
    def isHeat: Boolean = eventType == "h"
    def isSmoke: Boolean = eventType == "s"

    override def toString: String =
      s"${formatTime(timestamp)} | $eventType"
  }

  case class Alarm(timestamp: Timestamp, info: String) {
    override def toString: String =
      s"Alarm ${formatTime(timestamp)} | $info"
  }

  class CustomTrigger extends Trigger[Event, TimeWindow] {
    private val delegate = EventTimeTrigger.create()

    override def onElement(element: Event, timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult =
      if (element.asInstanceOf[Event].isSmoke) {
        TriggerResult.FIRE
      } else
        delegate.onElement(element, timestamp, window, ctx)

    override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult =
      delegate.onProcessingTime(time, window, ctx)

    override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult =
      delegate.onEventTime(time, window, ctx)

    override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit =
      delegate.clear(window, ctx)

    override def canMerge: Boolean =
      delegate.canMerge

    override def onMerge(window: TimeWindow, ctx: Trigger.OnMergeContext): Unit =
      delegate.onMerge(window, ctx)
  }

}
