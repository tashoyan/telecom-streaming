package com.github.tashoyan.telecom.predictor

import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.conditions.Context
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.test.util.AbstractTestBase
import org.junit.Test
import org.scalatest.junit.JUnitSuiteLike

import scala.collection.Map

@deprecated(message = "Remove", since = "now")
class CepTest extends AbstractTestBase with JUnitSuiteLike {

  @Test def cep1(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val input: DataStream[String] = env.fromCollection(Seq(
      "A",
      "B",
      "C",
      "D"
    ))

    val pattern = Pattern.begin[String]("start").where(_ == "A")
      .next("middle").where(_ == "B")
      .followedBy("end").where(_ == "D")
    val patternStream = CEP.pattern(input, pattern)
    val result: DataStream[String] = patternStream.select { foundPattern: Map[String, Iterable[String]] =>
      println(s"Found pattern: $foundPattern")
      s"Auchtung: $foundPattern"
    }
    result.print()

    env.execute()
    ()
  }

  @Test def fireAlarm(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val problemTimeout = 5L
    val input: DataStream[Evt] = env.fromCollection(Seq(
      Evt(1L, "heat"),
      Evt(2L, "heat"),
      Evt(3L, "smoke")
    ))

    val pattern = Pattern.begin[Evt]("heat").where(_.eventType == "heat")
      .followedBy("smoke").where { (event: Evt, ctx: Context[Evt]) =>
        lazy val latestHeat = ctx.getEventsForPattern("heat")
          .toSeq
          .sortBy(-_.ts)
          .headOption
        event.eventType == "smoke" && latestHeat.exists(event.ts - _.ts <= problemTimeout)
      }
    val patternStream = CEP.pattern(input, pattern)
    val alarms: DataStream[String] = patternStream.select { foundPattern: Map[String, Iterable[Evt]] =>
      val heat = foundPattern("heat").head
      val smoke = foundPattern("smoke").head
      s"Fire alarm: heat: $heat, smoke: $smoke"
    }
    alarms.print()

    env.execute()
    ()
  }

}

case class Evt(ts: Long, eventType: String) extends Serializable
