package com.github.tashoyan.telecom.event

import java.sql.Timestamp
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.github.tashoyan.telecom.event.AlarmJson.toJson
import org.scalatest.{FunSuite, Matchers}

class AlarmJsonTest extends FunSuite with Matchers {

  test("toJson - full alarm") {
    val alarm = Alarm(
      timestamp = new Timestamp(500L),
      objectId = 100L,
      severity = "CRITICAL",
      info = "Alarm 1"
    )
    val result = toJson(alarm)

    val parsedResult = jsonToMap(result)
    isoToEpochMillis(parsedResult("timestamp").toString) shouldBe 500L
    parsedResult("objectId") shouldBe 100L
    parsedResult("severity") shouldBe "CRITICAL"
    parsedResult("info") shouldBe "Alarm 1"
  }

  private def isoToEpochMillis(isoTimestamp: String): Long = {
    ZonedDateTime.parse(isoTimestamp, DateTimeFormatter.ISO_ZONED_DATE_TIME)
      .toInstant
      .toEpochMilli
  }

  private val mapper: ObjectMapper = {
    new ObjectMapper()
      .registerModule(DefaultScalaModule)
  }

  private def jsonToMap(json: String): Map[String, Any] = {
    mapper.readValue(json, classOf[Map[String, Any]])
  }

}
