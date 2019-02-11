package com.github.tashoyan.telecom.predictor

import java.sql.Timestamp

import com.github.tashoyan.telecom.event.Event
import com.github.tashoyan.telecom.event.Event._
import com.github.tashoyan.telecom.event.SparkEventAdapter.EventDataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{GroupState, OutputMode}

//scalastyle:off
object SparkPredictorLocal {
  private val port: Long = 9999

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
      column("value").contains("2"),
      concat_ws(" ", lit("Smoke on site"), col(siteIdColumn))
    )
      .otherwise(
        concat_ws(" ", lit("Heat on site"), col(siteIdColumn))
      )

    val events = input
      .withColumn(siteIdColumn, lit(1L))
      .withColumn(severityColumn, lit("MAJOR"))
      .withColumn(infoColumn, infoCol)
      .asEvents

    def dummyFunction: (Long, Iterator[Event], GroupState[DummyState]) => DummyState = {
      (siteId, events, state) =>
        if (state.exists) {
          println("EXISTING STATE")
          val smokeTimestamp = events.toStream
            .find(_.info.toLowerCase.contains("smoke"))
            .map(_.timestamp)
          if (smokeTimestamp.isDefined) {
            println(s"GOT SMOKE: $smokeTimestamp")
            val newState = DummyState(siteId, state.get.heatTimestamp, smokeTimestamp.get)
            state.update(newState)
            newState
          } else {
            println("NO SMOKE YET")
            state.get
          }
        } else {
          println("NEW STATE")
          val heatTimestamp = events.next().timestamp
          val newState = DummyState(siteId, heatTimestamp, null)
          state.update(newState)
          newState
        }
    }

    val alarms = events
      .groupByKey(_.siteId)
      .mapGroupsWithState(dummyFunction)
      .where(col("smokeTimestamp").isNotNull)

    val query = alarms.writeStream
      .outputMode(OutputMode.Update())
      .format("console")
      .option("truncate", value = false)
      .start()
    query.awaitTermination()
  }

}

case class DummyState(siteId: Long, heatTimestamp: Timestamp, smokeTimestamp: Timestamp)
