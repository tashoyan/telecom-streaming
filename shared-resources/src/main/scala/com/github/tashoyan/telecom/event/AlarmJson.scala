package com.github.tashoyan.telecom.event

import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write
import org.json4s.{Formats, NoTypeHints}

object AlarmJson {

  def toJson(alarm: Alarm): String = {
    implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)
    write(alarm)
  }

}
