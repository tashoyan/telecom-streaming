package com.github.tashoyan.telecom.topology

import com.github.tashoyan.telecom.test.SparkTestHarness
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.scalatest.FunSuite

class SampleGeneratorTest extends FunSuite with SparkTestHarness {

  test("controllers 2715, 2016 - all") {
    val topologyFile = getClass
      .getResource("topology_controller_station.parquet")
      .toString
    val topology = spark.read.parquet(topologyFile)

    topology.where(
      col("controller") === 2715 or
        col("controller") === 2716
    )
      .write
      .parquet("target/controllers_2715_2716_all")
  }

  test("controllers 2715, 2016 - half 1, 2") {
    val topologyFile = getClass
      .getResource("topology_controller_station.parquet")
      .toString
    val topology = spark.read.parquet(topologyFile)
    val window = Window.partitionBy("controller")
      .orderBy("station")
    val bothControllers = topology.where(
      col("controller") === 2715 or
        col("controller") === 2716
    )
      .withColumn("half", ntile(2) over window)

    Seq(1, 2).foreach { half =>
      bothControllers
        .where(col("half") === half)
        .write
        .parquet(s"target/controllers_2715_2716_half$half")
    }
  }

}
