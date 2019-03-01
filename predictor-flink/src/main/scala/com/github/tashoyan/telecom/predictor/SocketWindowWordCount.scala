package com.github.tashoyan.telecom.predictor

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/*
$ nc -lk 9999
$ mvn -pl :predictor-flink exec:java -Dexec.mainClass=com.github.tashoyan.telecom.predictor.SocketWindowWordCount -Dexec.args="--port 9999"
*/
object SocketWindowWordCount {
  private val windowSizeSec = 5L
  private val windowSlideSec = 1L

  def main(args: Array[String]): Unit = {
    val port: Int = try {
      ParameterTool.fromArgs(args).getInt("port")
    } catch {
      case e: Exception =>
        throw new RuntimeException(s"Cannot parse the port from args: '${args.mkString(",")}'", e)
    }

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val text = env.socketTextStream("localhost", port, '\n')

    val windowCounts: DataStream[WordCount] = text
      .flatMap(_.split("\\s+"))
      .map(WordCount(_, 1))
      .keyBy("word")
      .timeWindow(Time.seconds(windowSizeSec), Time.seconds(windowSlideSec))
      .sum("count")

    windowCounts.print()
      .setParallelism(1)

    env.execute(this.getClass.getSimpleName)
    ()
  }

  case class WordCount(word: String, count: Long)

}
