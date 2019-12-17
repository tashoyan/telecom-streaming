package com.github.tashoyan.telecom.test

import java.io.StringWriter
import java.time.ZonedDateTime

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.scalatest.matchers.{MatchResult, Matcher}

trait JsonTestHarness {

  private val mapper: ObjectMapper =
    new ObjectMapper()
      .registerModule(DefaultScalaModule)

  protected def jsonToMap(json: String): Map[String, Any] = {
    mapper.readValue(json, classOf[Map[String, Any]])
  }

  protected def mapToJson(map: Map[String, Any]): String = {
    val out = new StringWriter()
    mapper.writeValue(out, map)
    out.toString
  }

  protected def matchEpochMillis(expectedEpochMillis: Long): TimestampEpochMillisMatcher = {
    new TimestampEpochMillisMatcher(expectedEpochMillis)
  }

  class TimestampEpochMillisMatcher(expectedEpochMillis: Long) extends Matcher[Any] {

    override def apply(left: Any): MatchResult = {
      val leftEpochMillis = ZonedDateTime.parse(left.toString)
        .toInstant
        .toEpochMilli
      MatchResult(
        leftEpochMillis == expectedEpochMillis,
        s"Epoch millis $leftEpochMillis of the timestamp '$left' did not match the expected epoch millis: $expectedEpochMillis",
        s"Epoch millis $leftEpochMillis of the timestamp '$left' matched the expected epoch millis: $expectedEpochMillis"
      )
    }

  }

}
