package com.github.tashoyan.telecom.util

import java.sql.Timestamp

object Timestamps {

  def yearMonth(ts: Timestamp): Int = {
    val localDateTime = ts.toLocalDateTime
    localDateTime.getYear * 100 + localDateTime.getMonthValue
  }

}
