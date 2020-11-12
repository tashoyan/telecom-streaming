package com.github.tashoyan.telecom.util

import java.sql.Timestamp
import java.time.LocalDateTime

import com.github.tashoyan.telecom.util.Timestamps._
import org.scalatest.funsuite.AnyFunSuite

class TimestampsTest extends AnyFunSuite {

  test("yearMonth") {
    val ts = Timestamp.valueOf(LocalDateTime.of(2018, 1, 31, 10, 15, 35))
    val result = yearMonth(ts)
    assert(result === 201801)
  }

}
