package com.github.tashoyan.telecom.predictor

import java.sql.Timestamp
import java.time.format.DateTimeFormatterBuilder
import java.time.temporal.ChronoField.{MINUTE_OF_HOUR, SECOND_OF_MINUTE}

import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{Dataset, SparkSession}

@deprecated(message = "to remove", since = "now")
object SparkSocketWindowWordCount {
  private val port = 9998L
  private val windowSizeSec = 5L
  private val windowSlideSec = 5L
  private val watermarkSec = 5L

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .getOrCreate()
    spark.sparkContext
      .setLogLevel("WARN")
    import spark.implicits._

    val timestampText = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", port)
      .option("includeTimestamp", value = true)
      .load()
      .as[(String, Timestamp)]
      .filter(_._1.nonEmpty)

    val timestampWords: Dataset[TimestampWord] = timestampText
      .flatMap { case (text, timestamp) =>
        text
          .split("\\s+")
          .map(TimestampWord(timestamp, _))
      }

    val windowCounts = timestampWords
      .withWatermark("timestamp", s"$watermarkSec seconds")
      .groupBy(
        col("word"),
        window(col("timestamp"), s"$windowSizeSec seconds", s"$windowSlideSec seconds")
      )
      .count()
      .select(
        col("window.start") as "windowStart",
        col("window.end") as "windowEnd",
        //Cannot use current_timestamp() due to https://issues.apache.org/jira/browse/SPARK-26379
        expr("null") as "emitTimestamp",
        col("word"),
        col("count")
      )
      .as[WindowWordCount]
      .map { _.copy(emitTimestamp = new Timestamp(System.currentTimeMillis())) }
      .map(_.toString)

    val query = windowCounts.writeStream
      .outputMode(OutputMode.Update())
      .format("console")
      .option("truncate", value = false)
      .start()
    query.awaitTermination()
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
