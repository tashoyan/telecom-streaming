package com.github.tashoyan.telecom.topology

import com.github.tashoyan.telecom.test.SparkTestHarness
import org.apache.spark.sql.functions._
import org.graphframes.GraphFrame
import org.scalatest.FunSuite

class DummyGraphTest extends FunSuite with SparkTestHarness {

  ignore("connected components") {
    val topologyFile = getClass
      .getResource("topology_controller_station.parquet")
      .toString
    val topology = spark.read.parquet(topologyFile)
    topology.show()
    println(s"Distinct stations: ${topology.select("station").distinct().count()}")
    println(s"Distinct controllers: ${topology.select("controller").distinct().count()}")

    val graph = GraphFrame.fromEdges(topology.select(col("station") as "src", col("controller") as "dst"))
    graph.triplets.show(false)

    spark.sparkContext.setCheckpointDir("checkpoint")
    val connectedComponents = graph.connectedComponents.run()
    connectedComponents.show(false)
    println(s"Distinct ids: ${connectedComponents.select("id").distinct().count()}")
    println(s"Distinct components: ${connectedComponents.select("component").distinct().count()}")
  }

}
