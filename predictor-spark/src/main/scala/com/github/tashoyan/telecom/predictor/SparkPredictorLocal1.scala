package com.github.tashoyan.telecom.predictor

import java.sql.Timestamp
import java.util.concurrent.TimeUnit

import com.github.tashoyan.telecom.event.Event
import com.github.tashoyan.telecom.event.Event._
import com.github.tashoyan.telecom.event.SparkEventAdapter.EventDataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.types.LongType

//scalastyle:off
object SparkPredictorLocal1 {
  private val port: Long = 9999
  private val watermarkIntervalSec = 20L
  private val problemTimeoutMillis = TimeUnit.SECONDS.toMillis(10L)

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

    val infoCol = when(
      col("input_words")(1).contains("2"),
      concat_ws(" ", lit("Smoke on site"), col(siteIdColumn))
    )
      .otherwise(
        concat_ws(" ", lit("Heat on site"), col(siteIdColumn))
      )

    val events = input
      .withColumn("input_words", split(col("value"), "\\s+"))
      .withColumn(siteIdColumn, col("input_words")(0) cast LongType)
      .withColumn(severityColumn, lit("MAJOR"))
      .withColumn(infoColumn, infoCol)
      .asEvents

    def isHeat(event: Event): Boolean =
      event.info != null &&
        event.info.toLowerCase.contains("heat")

    def isSmoke(event: Event): Boolean =
      event.info != null &&
        event.info.toLowerCase.contains("smoke")

    def dummyAlarmFunction: (Long, Iterator[Event], GroupState[DummyState1]) => Iterator[DummyAlarm1] = {
      (siteId, events, state) =>
        if (state.hasTimedOut) {
          println(s"TIMED OUT STATE on $siteId")
          val heatTimestamp = state.getOption.map(_.heatTimestamp)
          println(s" -- heatTimestamp: $heatTimestamp")
          println(s" -- now: ${new Timestamp(System.currentTimeMillis())}")
          state.remove()
          Iterator.empty
        } else if (state.exists) {
          println(s"EXISTING STATE on $siteId")
          val heatTimestamp = state.get.heatTimestamp
          println(s" -- heatTimestamp: $heatTimestamp")
          val smokeTimestamp = events.toStream
            .find(isSmoke)
            .map(_.timestamp)
          val timeoutTimestamp = heatTimestamp.getTime + problemTimeoutMillis
          state.setTimeoutTimestamp(timeoutTimestamp)

          if (smokeTimestamp.isDefined) {
            println(s" -- smokeTimestamp: $smokeTimestamp")
            val smokeTs = smokeTimestamp.get
            if (smokeTs.getTime - heatTimestamp.getTime > 0 &&
              smokeTs.getTime - heatTimestamp.getTime <= problemTimeoutMillis) {
              val alarm = DummyAlarm1(siteId, heatTimestamp, smokeTs, s"Fire on $siteId")
              state.remove()
              Iterator(alarm)
            } else {
              println(" -- smoke is too late")
              state.remove()
              Iterator.empty
            }
          } else {
            println(" -- no smoke yet")
            Iterator.empty
          }
        } else {
          println(s"NEW STATE on $siteId")
          val heatTimestamp = events.toStream
            .find(isHeat)
            .map(_.timestamp)
          heatTimestamp.foreach { heatTs =>
            println(s" -- heatTimestamp: $heatTs")
            val newState = DummyState1(siteId, heatTs)
            state.update(newState)
            val timeoutTimestamp = heatTs.getTime + problemTimeoutMillis
            state.setTimeoutTimestamp(timeoutTimestamp)
            println(s" -- timeoutTimestamp: ${new Timestamp(timeoutTimestamp)}")
            println(s" -- watermark timeout timestamp: ${new Timestamp(timeoutTimestamp + TimeUnit.SECONDS.toMillis(watermarkIntervalSec))}")
          }
          Iterator.empty
        }
    }

    val alarms = events
      .withWatermark(timestampColumn, s"$watermarkIntervalSec seconds")
      .groupByKey(_.siteId)
      .flatMapGroupsWithState(OutputMode.Update(), GroupStateTimeout.EventTimeTimeout())(dummyAlarmFunction)

    val query = alarms.writeStream
      .outputMode(OutputMode.Update())
      .format("console")
      .option("truncate", value = false)
      .start()
    query.awaitTermination()
  }

}

case class DummyState1(siteId: Long, heatTimestamp: Timestamp)

case class DummyAlarm1(siteId: Long, heatTimestamp: Timestamp, smokeTimestamp: Timestamp, info: String)
