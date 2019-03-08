package com.github.tashoyan.telecom.predictor

import java.sql.Timestamp
import java.time.format.DateTimeFormatterBuilder
import java.time.temporal.ChronoField._

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.extensions._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/*
Ways to implement the predictor:
1. Session window:
 https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/operators/windows.html#session-windows
2. State - per-key, per-window:
 https://ci.apache.org/projects/flink/flink-docs-release-1.7/dev/stream/state/state.html
 State TTL - cleanup on timeout
3. CEP:
 https://ci.apache.org/projects/flink/flink-docs-release-1.7/dev/libs/cep.html
*/
/*
$ nc -lk 9999
$ mvn -pl :predictor-flink exec:java -Dexec.mainClass=com.github.tashoyan.telecom.predictor.FlinkSocketWindowWordCount -Dexec.args="--port 9999"
*/
//scalastyle:off
@deprecated(message = "to remove", since = "now")
object FlinkSocketWindowWordCount {
  private val windowSizeMillis = 5000L
  private val windowSlideMillis = 5000L
  private val eventOutOfOrdernessMillis = 5000L
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
    //TODO Buffer timeout 0 iz very aggressive - don't do this
    env.setBufferTimeout(0L)
    env.getConfig.setAutoWatermarkInterval(watermarkCheckIntervalMillis)

    val timestampText: DataStream[(String, Timestamp)] = env.socketTextStream("localhost", port, '\n')
      .filter(_.nonEmpty)
      .map((_, new Timestamp(System.currentTimeMillis())))

    val timestampWords: DataStream[TimestampWord] = timestampText.flatMapWith { case (text, timestamp) =>
      text
        .split("\\s+")
        .map(TimestampWord(timestamp, _))
    }
    //TODO Why setParallelism() affects the parallelism only if invoked after print()?
    timestampWords.print()
      .setParallelism(1)

    //TODO What is the preferred way to assign watermarks?
    val wmAssigner = new BoundedOutOfOrdernessTimestampExtractor[TimestampWord](Time.milliseconds(eventOutOfOrdernessMillis)) {
      override def extractTimestamp(tsWord: TimestampWord): Long = tsWord.timestamp.getTime
    }

    val preAggregator = new AggregateFunction[TimestampWord, Long, Long] {
      override def createAccumulator(): Long = 0L

      override def add(tsWord: TimestampWord, acc: Long): Long = acc + 1

      override def merge(acc1: Long, acc2: Long): Long = acc1 + acc2

      override def getResult(acc: Long): Long = acc
    }

    val windowFunction: ProcessWindowFunction[Long, WindowWordCount, String, TimeWindow] = new ProcessWindowFunction[Long, WindowWordCount, String, TimeWindow] {
      override def process(key: String, context: Context, elements: Iterable[Long], out: Collector[WindowWordCount]): Unit =
        out.collect(
          WindowWordCount(
            new Timestamp(context.window.getStart),
            new Timestamp(context.window.getEnd),
            new Timestamp(System.currentTimeMillis()),
            key,
            elements.head
          )
        )
    }

    /*
    TODO If the source does not send the data, then operations based on event time cannot progress.
     https://ci.apache.org/projects/flink/flink-docs-release-1.7/dev/event_time.html#idling-sources
     Kafka notes:
     https://ci.apache.org/projects/flink/flink-docs-stable/dev/connectors/kafka.html#kafka-consumers-and-timestamp-extractionwatermark-emission
     Workaround: send heartbeat messages (poor workaround, garbage on the wire)
    */
    val windowCounts: DataStream[WindowWordCount] = timestampWords
      .assignTimestampsAndWatermarks(wmAssigner)
      .keyBy(_.word)
      .timeWindow(Time.milliseconds(windowSizeMillis), Time.milliseconds(windowSlideMillis))
      .aggregate[Long, Long, WindowWordCount](
        preAggregator,
        windowFunction
      )

    windowCounts.print()
      .setParallelism(1)

    env.execute(this.getClass.getSimpleName)
    ()
  }

  case class TimestampWord(timestamp: Timestamp, word: String) {
    import TimestampFormat._

    override def toString: String =
      s"${formatTime(timestamp)} | $word"
  }

  case class WindowWordCount(
      windowStart: Timestamp,
      windowEnd: Timestamp,
      emitTimestamp: Timestamp,
      word: String,
      count: Long
  ) {
    import TimestampFormat._

    override def toString: String =
      s"[${formatTime(windowStart)}, ${formatTime(windowEnd)}] | ${formatTime(emitTimestamp)} | $word : $count"

  }

  object TimestampFormat {
    private val formatter = new DateTimeFormatterBuilder()
      .appendValue(MINUTE_OF_HOUR, 2)
      .appendLiteral(':')
      .appendValue(SECOND_OF_MINUTE, 2)
      .toFormatter()

    def formatTime(ts: Timestamp): String =
      ts.toLocalDateTime
        .format(formatter)
  }

}
