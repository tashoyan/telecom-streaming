package com.github.tashoyan.telecom.flink

import java.nio.charset.StandardCharsets

import com.github.tashoyan.telecom.event.Alarm
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema
import org.json4s.native.Serialization
import org.json4s.native.Serialization._
import org.json4s.{Formats, NoTypeHints}

class AlarmSerializationSchema extends KeyedSerializationSchema[Alarm] {

  /*
  All alarms having same objectId go to the same Kafka partition
  */
  override def serializeKey(alarm: Alarm): Array[Byte] =
    Array(alarm.objectId.toByte)

  override def serializeValue(alarm: Alarm): Array[Byte] =
    toJson(alarm)
      .getBytes(StandardCharsets.UTF_8)

  private def toJson(alarm: Alarm): String = {
    implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)
    write(alarm)
  }

  /*
  This is the contract: return null if no target topic.
  */
  //scalastyle:off null
  override def getTargetTopic(alarm: Alarm): String = null
  //scalastyle:on

}
