package com.github.tashoyan.telecom.predictor

import java.sql.Timestamp
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.extensions._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.{TimeWindow, Window}
import org.apache.flink.util.Collector

/*
$ nc -lk 9999
$ mvn -pl :predictor-flink exec:java -Dexec.mainClass=com.github.tashoyan.telecom.predictor.SocketWindowWordCount -Dexec.args="--port 9999"
*/
//scalastyle:off
object SocketWindowWordCount {
  private val windowSizeSec = 5L
  private val windowSlideSec = 5L
  private val watermarkSec = 5L
  private val watermarkCheckIntervalSec = 1L

  def main(args: Array[String]): Unit = {
    val port: Int = try {
      ParameterTool.fromArgs(args).getInt("port")
    } catch {
      case e: Exception =>
        throw new RuntimeException(s"Cannot parse the port from args: '${args.mkString(",")}'", e)
    }

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(TimeUnit.SECONDS.toMillis(watermarkCheckIntervalSec))

    val text: DataStream[String] = env.socketTextStream("localhost", port, '\n')
    val tsText: DataStream[(Timestamp, String)] = text
      .map { input =>
        val inputSplit = input.split("\\s+", 2)
        val (delayStr, words) = (inputSplit.head, inputSplit.tail.head)
        val delaySec = delayStr.toLong
        val timestamp = new Timestamp(System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(delaySec))
        (timestamp, words)
      }

    val tsWords: DataStream[(Timestamp, String)] = tsText.flatMapWith { case (timestamp, words) =>
      words
        .split("\\s+")
        .map((timestamp, _))
    }

    val wmAssigner = new BoundedOutOfOrdernessTimestampExtractor[(Timestamp, String)](Time.seconds(watermarkSec)) {
      override def extractTimestamp(element: (Timestamp, String)): Long = element._1.getTime
    }

    val preAggregator = new AggregateFunction[(Timestamp, String), (String, Long), (String, Long)] {
      override def createAccumulator(): (String, Long) = ("", 0L)

      override def add(value: (Timestamp, String), accumulator: (String, Long)): (String, Long) = (value._2, accumulator._2 + 1)

      override def getResult(accumulator: (String, Long)): (String, Long) = accumulator

      override def merge(a: (String, Long), b: (String, Long)): (String, Long) = (a._1, a._2 + b._2)
    }

    val windowFunction = new WindowFunction[(String, Long), WindowWordCount, String, TimeWindow] {
      override def apply(key: String, window: TimeWindow, input: Iterable[(String, Long)], out: Collector[WindowWordCount]): Unit =
        out.collect(WindowWordCount(new Timestamp(window.getStart), new Timestamp(window.getEnd), key, input.map(_._2).sum))
    }

    val windowCounts: DataStream[WindowWordCount] = tsWords
      .assignTimestampsAndWatermarks(wmAssigner)
      .keyBy(_._2)
      .timeWindow(Time.seconds(windowSizeSec), Time.seconds(windowSlideSec))
      .aggregate[(String, Long), (String, Long), WindowWordCount](
        preAggregator,
        windowFunction
      )

    windowCounts.print()
      .setParallelism(1)

    env.execute(this.getClass.getSimpleName)
    ()
  }

  case class WindowWordCount(start: Timestamp, end: Timestamp, word: String, count: Long)

}
