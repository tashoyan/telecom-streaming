package com.github.tashoyan.telecom.sampler

import java.sql.Timestamp
import java.util.concurrent.TimeUnit

import com.github.tashoyan.telecom.event.Event
import com.github.tashoyan.telecom.sampler.Sampler._
import com.github.tashoyan.telecom.test.SparkTestHarness
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.scalatest.FunSuite

class SampleGeneratorTest extends FunSuite with SparkTestHarness {

  test("event schema") {
    val spark0 = spark
    import spark0.implicits._

    val empty = spark.emptyDataset[Event]
    writeEvents(empty, "target/event_schema")
  }

  test("communication_events-controllers_2715_2716_all-1min-uniq") {
    val spark0 = spark
    import spark0.implicits._

    val topologyFile = this.getClass
      .getResource("topology_controller_station.parquet")
      .toString
    val topology = spark.read.parquet(topologyFile)

    val stations = topology.where(
      col("controller") === 2715 or
        col("controller") === 2716
    )
      .select("station")
      .as[Int]
      .collect()

    val events = generateEvents(
      stations = stations,
      timeRangeMillis = TimeUnit.MINUTES.toMillis(1),
      perStationMultiplier = 1,
      severity = "MAJOR",
      info = "Communication failure"
    )
      .toDS()
    writeEvents(events, "target/communication_events-controllers_2715_2716_all-1min-uniq")
  }

  test("communication_events-controllers_2715_2716_all-1min-dup") {
    val spark0 = spark
    import spark0.implicits._

    val topologyFile = this.getClass
      .getResource("topology_controller_station.parquet")
      .toString
    val topology = spark.read.parquet(topologyFile)

    val stations = topology.where(
      col("controller") === 2715 or
        col("controller") === 2716
    )
      .select("station")
      .as[Int]
      .collect()

    val events = generateEvents(
      stations = stations,
      timeRangeMillis = TimeUnit.MINUTES.toMillis(1),
      perStationMultiplier = 2,
      severity = "MAJOR",
      info = "Communication failure"
    )
      .toDS()
    writeEvents(events, "target/communication_events-controllers_2715_2716_all-1min-dup")
  }

  test("communication_events-controllers_2715_2716_half1/2-1min-dup") {
    val spark0 = spark
    import spark0.implicits._

    val topologyFile = this.getClass
      .getResource("topology_controller_station.parquet")
      .toString
    val topology = spark.read.parquet(topologyFile)
    val window = Window.partitionBy("controller")
      .orderBy("station")
    //Total: 242 = 126[2715] + 116[2716]
    val bothControllers = topology.where(
      col("controller") === 2715 or
        col("controller") === 2716
    )
      .withColumn("half", ntile(2) over window)

    Seq(1, 2).foreach { half =>
      val stations = bothControllers
        .where(col("half") === half)
        .select("station")
        .as[Int]
        .collect()
      val events = generateEvents(
        stations = stations,
        timeRangeMillis = TimeUnit.MINUTES.toMillis(1),
        perStationMultiplier = 2,
        severity = "MAJOR",
        info = "Communication failure"
      )
        .toDS()
      writeEvents(events, s"target/communication_events-controllers_2715_2716_half$half-1min-dup")
    }
  }

  test("heat_smoke_events-site_1-15sec") {
    val spark0 = spark
    import spark0.implicits._

    val siteId = 1L
    val events = Seq(
      Event(new Timestamp(0L), siteId, "MAJOR", s"Smoke on site $siteId"),
      Event(new Timestamp(15000L), siteId, "MINOR", s"Heat on site $siteId")
    )
      .toDS()
    writeEvents(events, "target/heat_smoke_events-site_1-15sec")
  }

  test("heat_events-site_1-1min-10_events") {
    val spark0 = spark
    import spark0.implicits._

    val siteId = 1
    val events = generateEvents(
      stations = Seq(siteId),
      timeRangeMillis = TimeUnit.MINUTES.toMillis(1),
      perStationMultiplier = 10,
      severity = "MAJOR",
      info = s"Heat on site $siteId"
    )
      .toDS()
    writeEvents(events, "target/heat_events-site_1-1min-10_events")
  }

  test("smoke_events-site_1-1min-10_events") {
    val spark0 = spark
    import spark0.implicits._

    val siteId = 1
    val events = generateEvents(
      stations = Seq(siteId),
      timeRangeMillis = TimeUnit.MINUTES.toMillis(1),
      perStationMultiplier = 10,
      severity = "MAJOR",
      info = s"Smoke on site $siteId"
    )
      .toDS()
    writeEvents(events, "target/smoke_events-site_1-1min-10_events")
  }

}
