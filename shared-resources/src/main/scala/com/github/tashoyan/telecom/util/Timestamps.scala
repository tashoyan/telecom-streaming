package com.github.tashoyan.telecom.util

import java.sql.Timestamp
import java.time.{Instant, ZoneId, ZonedDateTime}
import java.time.format.DateTimeFormatter

object Timestamps {

  def yearMonth(ts: Timestamp): Int = {
    val localDateTime = ts.toLocalDateTime
    localDateTime.getYear * 100 + localDateTime.getMonthValue
  }

  def formattedTimestamp(epochMillis: Long): String = {
    ZonedDateTime.ofInstant(
      Instant.ofEpochMilli(epochMillis),
      ZoneId.systemDefault()
    )
      .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)
  }

  implicit class RichTimestamp(val timestamp: Timestamp) extends AnyVal with Ordered[Timestamp] {

    @inline override def compare(that: Timestamp): Int = timestamp compareTo that

    @inline def notBefore(that: Timestamp): Boolean = !(timestamp before that)

    @inline def notAfter(that: Timestamp): Boolean = !(timestamp after that)
  }

}
