package com.github.tashoyan.telecom.topology

import java.sql.Timestamp

import com.github.tashoyan.telecom.event.Event
import com.github.tashoyan.telecom.event.Event._
import com.github.tashoyan.telecom.test.SparkTestHarness
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.scalatest.FunSuite

//TODO Move to a special module - event-sampler or so
class SampleGeneratorTest extends FunSuite with SparkTestHarness {

  private val eventsPerStation = 2

  private def generateEvents(stations: Dataset[Integer]): Dataset[Event] = {
    val spark0 = spark
    import spark0.implicits._

    val stationCount = eventsPerStation
    val stationsUdf = udf { station: Integer =>
      Seq.fill(stationCount)(station.toLong)
    }
    val infoUdf = udf { siteId: Long =>
      s"Communication failure at site $siteId"
    }

    stations
      .withColumn("stations", stationsUdf(col("station")))
      .withColumn(siteIdColumn, explode(col("stations")))
      .withColumn(idColumn, monotonically_increasing_id())
      //Let the Event Generator set timestamps of the generated events
      .withColumn(timestampColumn, lit(new Timestamp(0L)))
      .withColumn(severityColumn, lit("MAJOR"))
      .withColumn(infoColumn, infoUdf(col(siteIdColumn)))
      .drop("station", "stations")
      .as[Event]
  }

  private def writeEvents(events: Dataset[Event], path: String): Unit = {
    events
      .repartition(1)
      .write
      .parquet(path)
  }

  test("controllers 2715, 2016 - all") {
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
      .as[Integer]

    val events = generateEvents(stations)
    writeEvents(events, "target/events_controllers_2715_2716_all")
  }

  test("controllers 2715, 2016 - half 1, 2") {
    val spark0 = spark
    import spark0.implicits._

    val topologyFile = this.getClass
      .getResource("topology_controller_station.parquet")
      .toString
    val topology = spark.read.parquet(topologyFile)
    val window = Window.partitionBy("controller")
      .orderBy("station")
    //Total: 242 = 126 + 116
    val bothControllers = topology.where(
      col("controller") === 2715 or
        col("controller") === 2716
    )
      .withColumn("half", ntile(2) over window)

    Seq(1, 2).foreach { half =>
      val stations = bothControllers
        .where(col("half") === half)
        .select("station")
        .as[Integer]
      val events = generateEvents(stations)
      writeEvents(events, s"target/events_controllers_2715_2716_half$half")
    }
  }

}
