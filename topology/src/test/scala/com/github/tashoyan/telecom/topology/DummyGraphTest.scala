package com.github.tashoyan.telecom.topology

import com.github.tashoyan.test.SparkTestHarness
import org.scalatest.FunSuite

class DummyGraphTest extends FunSuite with SparkTestHarness {

  test("connected components") {
    val topologyFile = getClass
      .getResource("topology_controller_station.parquet")
      .toString
    val topology = spark.read.parquet(topologyFile)
    topology.show()
  }

}
