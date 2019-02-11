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

  test("controllers 2715, 2016 - all - 1 min - unique") {
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

    val events = generateEvents(stations, TimeUnit.MINUTES.toMillis(1), 1)
      .toDS()
    writeEvents(events, "target/events_controllers_2715_2716_all_1min_uniq")
  }

  test("controllers 2715, 2016 - all - 1 min - duplicates") {
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

    val events = generateEvents(stations, TimeUnit.MINUTES.toMillis(1), 2)
      .toDS()
    writeEvents(events, "target/events_controllers_2715_2716_all_1min_dup")
  }

  test("controllers 2715, 2016 - half 1, 2 - 1 min - duplicates") {
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
      val events = generateEvents(stations, TimeUnit.MINUTES.toMillis(1), 2)
        .toDS()
      writeEvents(events, s"target/events_controllers_2715_2716_half${half}_1min_dup")
    }
  }

  test("site 1 - heat and smoke - 15 sec interval") {
    val spark0 = spark
    import spark0.implicits._

    val siteId = 1L
    val events = Seq(
      Event(new Timestamp(0L), siteId, "MAJOR", s"Smoke on site $siteId"),
      Event(new Timestamp(15000L), siteId, "MINOR", s"Heat on site $siteId")
    )
      .toDS()
    writeEvents(events, "target/events_site_1_heat_and_smoke_15sec")
  }

}
