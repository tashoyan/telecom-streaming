package com.github.tashoyan.telecom.predictor

import java.sql.Timestamp
import java.util.concurrent.TimeUnit

import com.github.tashoyan.telecom.event.Event._
import com.github.tashoyan.telecom.event.SparkEventAdapter.EventDataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode}

//TODO Enable scalastyle back
//scalastyle:off
object SparkPredictorLocal {
  private val port: Long = 9999
  private val watermarkIntervalSec = 20L
  private val problemTimeoutSec = 10L

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession.builder()
      .getOrCreate()
    spark.sparkContext
      .setLogLevel("WARN")
    import spark.implicits._

    val input = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", port)
      .option("includeTimestamp", value = true)
      .load()

    def parseInputString(inputStr: String): Seq[(Long, String)] = {
      inputStr
        .split("\\s+")
        .grouped(2)
        .flatMap {
          case Array(num, token) => Iterator((num.toLong, token))
          case _ => Iterator.empty
        }
        .toSeq
    }

    def generateEventInfo(token: String): String =
      if (token.contains("2")) "Smoke on site"
      else "Heat on site"

    val events = input
      .as[(Timestamp, String)]
      .flatMap { case (timestamp, inputStr) =>
        val siteEvents: Seq[(Long, String)] = parseInputString(inputStr)
        siteEvents.map { case (siteId, token) =>
          (timestamp, siteId, "MAJOR", generateEventInfo(token))
        }
      }
      .toDF(columns: _*)
      .asEvents

    val alarmStateFunction = new FireAlarmStateFunction(TimeUnit.SECONDS.toMillis(problemTimeoutSec))
    val alarms = events
      .withWatermark(timestampColumn, s"$watermarkIntervalSec seconds")
      .groupByKey(_.siteId)
      .flatMapGroupsWithState(OutputMode.Update(), GroupStateTimeout.EventTimeTimeout())(alarmStateFunction.updateAlarmState)

    val query = alarms.writeStream
      .outputMode(OutputMode.Update())
      .format("console")
      .option("truncate", value = false)
      .start()
    query.awaitTermination()
  }

}
