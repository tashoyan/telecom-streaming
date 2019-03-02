package com.github.tashoyan.telecom.predictor

import java.sql.Timestamp
import java.util.concurrent.TimeUnit

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.extensions._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.{TimeWindow, Window}
import org.apache.flink.util.Collector

/*
$ nc -lk 9999
$ mvn -pl :predictor-flink exec:java -Dexec.mainClass=com.github.tashoyan.telecom.predictor.SocketWindowWordCount -Dexec.args="--port 9999"
*/
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

    val windowCounts: DataStream[WordCount] = tsWords
      .assignTimestampsAndWatermarks(wmAssigner)
      .map(tsWord => WordCount(tsWord._2, new TimeWindow(0, 0), 1))
      .keyBy(_.word)
      .timeWindow(Time.seconds(windowSizeSec), Time.seconds(windowSlideSec))
      .reduce[WordCount](
        (c1: WordCount, c2: WordCount) =>
          WordCount(c1.word, c1.window, c1.count + c2.count),
        (key: String, window: Window, counts: Iterable[WordCount], collector: Collector[WordCount]) =>
          collector.collect(WordCount(key, window, counts.map(_.count).sum))
      )

    windowCounts.print()
      .setParallelism(1)

    env.execute(this.getClass.getSimpleName)
    ()
  }

  case class WordCount(word: String, window: Window, count: Long)

}
