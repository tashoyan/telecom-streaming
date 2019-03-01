package com.github.tashoyan.telecom.predictor

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.streaming.connectors.wikiedits.{WikipediaEditEvent, WikipediaEditsSource}

/*
API to implement the Predictor:
https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/operators/process_function.html
https://ci.apache.org/projects/flink/flink-docs-release-1.7/dev/libs/cep.html
DataStream? Table? SQL?

Getting started:
https://ci.apache.org/projects/flink/flink-docs-release-1.7/tutorials/datastream_api.html#writing-a-flink-program
https://ci.apache.org/projects/flink/flink-docs-release-1.7/tutorials/local_setup.html
*/
object WikipediaAnalysis {
  private val windowSizeSec = 5L

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //TODO By default sources have a parallelism of 1 - learn parallel source
    val sourceFunction: SourceFunction[WikipediaEditEvent] = new WikipediaEditsSource()
    val edits: DataStream[WikipediaEditEvent] = env.addSource(sourceFunction)
    val keyedEdits: KeyedStream[WikipediaEditEvent, String] = edits.keyBy(_.getUser)

    //TODO Better implementation: https://stackoverflow.com/questions/47123785/flink-how-to-convert-the-deprecated-fold-to-aggregrate
    val aggregateFunction: AggregateFunction[WikipediaEditEvent, (String, Long), (String, Long)] =
      new AggregateFunction[WikipediaEditEvent, (String, Long), (String, Long)] {
        override def createAccumulator(): (String, Long) =
          ("", 0L)

        override def add(event: WikipediaEditEvent, acc: (String, Long)): (String, Long) =
          (event.getUser, acc._2 + event.getByteDiff)

        override def getResult(accumulator: (String, Long)): (String, Long) =
          accumulator

        override def merge(a: (String, Long), b: (String, Long)): (String, Long) =
          (a._1, a._2 + b._2)
      }
    val result: DataStream[(String, Long)] = keyedEdits
      .timeWindow(Time.seconds(windowSizeSec))
      .aggregate(aggregateFunction)

    //    result.print()
    result
      .map(_.toString())
      .addSink(new FlinkKafkaProducer[String]("ossv147.gre.hpecorp.net:9092", "events", new SimpleStringSchema()))

    env.execute(this.getClass.getSimpleName)
    //TODO Track execution?
    ()
  }

}
