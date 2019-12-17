package com.github.tashoyan.telecom.event

import java.sql.Timestamp

import com.github.tashoyan.telecom.event.AlarmJson.toJson
import com.github.tashoyan.telecom.test.JsonTestHarness
import org.scalatest.{FunSuite, Matchers}

class AlarmJsonTest extends FunSuite with Matchers with JsonTestHarness {

  test("toJson - full alarm") {
    val alarm = Alarm(
      timestamp = new Timestamp(500L),
      objectId = 100L,
      severity = "CRITICAL",
      info = "Alarm 1"
    )
    val result = toJson(alarm)

    val parsedResult = jsonToMap(result)
    parsedResult("timestamp") should matchEpochMillis(500L)
    parsedResult("objectId") shouldBe 100L
    parsedResult("severity") shouldBe "CRITICAL"
    parsedResult("info") shouldBe "Alarm 1"
  }

}
