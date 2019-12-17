package com.github.tashoyan.telecom.event

import org.json4s.native.Serialization
import org.json4s.native.Serialization._
import org.json4s.{Formats, NoTypeHints}

object AlarmJson {

  def toJson(alarm: Alarm): String = {
    implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)
    write(alarm)
  }

}
