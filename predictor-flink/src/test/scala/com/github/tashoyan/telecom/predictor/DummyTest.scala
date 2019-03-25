package com.github.tashoyan.telecom.predictor

import org.apache.flink.streaming.api.datastream.DataStreamUtils
import org.scalatest.FunSuite

//TODO Remove
@deprecated(message = "Remove", since = "now")
class DummyTest extends FunSuite {

  test("DataSet from collection") {
    import org.apache.flink.api.scala._

    val env = ExecutionEnvironment.getExecutionEnvironment
    val input = env.fromCollection(Seq(
      "aa",
      "bb",
      "cc",
      "dd"
    ))
    val result = input.filter(_ != "bb")
    result.print()
  }

  /*
  TODO Testing should be much easier than with Spark Streaming:
   Can create DataStream from a collection
   Can output DataStream to an iterator
  */
  test("DataStream from collection") {
    import org.apache.flink.streaming.api.scala._

    import scala.collection.JavaConverters.asScalaIteratorConverter

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val input = env.fromCollection(Seq(
      "aa",
      "bb",
      "cc",
      "dd"
    ))
    val result = input.filter(_ != "cc")
    result.print().setParallelism(2)

    env.execute()

    val output: Iterator[String] = DataStreamUtils.collect(result.javaStream).asScala
    output.foreach(str => println(s"str: $str"))
  }

  test("Pattern matching on DataSet") {
    import org.apache.flink.api.scala._
    import org.apache.flink.api.scala.extensions._

    val env = ExecutionEnvironment.getExecutionEnvironment
    val input: DataSet[(String, Int)] = env.fromCollection(Seq(
      ("aa", 1),
      ("bb", 2),
      ("cc", 3),
      ("dd", 4)
    ))
    val result = input.mapWith { case (name, idx) =>
      name + idx.toString
    }
    result.print()
  }

}
