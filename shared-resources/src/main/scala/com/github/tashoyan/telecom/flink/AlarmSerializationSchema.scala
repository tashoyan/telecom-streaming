package com.github.tashoyan.telecom.flink

import java.lang.{Long => JLong}
import java.nio.charset.StandardCharsets

import com.github.tashoyan.telecom.event.Alarm
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema
import org.apache.kafka.clients.producer.ProducerRecord
import org.json4s.native.Serialization
import org.json4s.native.Serialization._
import org.json4s.{Formats, NoTypeHints}

class AlarmSerializationSchema(kafkaTopic: String) extends KafkaSerializationSchema[Alarm] {

  override def serialize(alarm: Alarm, timestamp: JLong): ProducerRecord[Array[Byte], Array[Byte]] = {
    /* All alarms having same objectId go to the same Kafka partition */
    val key = Array(alarm.objectId.toByte)
    val value = toJson(alarm)
      .getBytes(StandardCharsets.UTF_8)
    new ProducerRecord[Array[Byte], Array[Byte]](
      kafkaTopic,
      /* Assign topic partitions according to message keys:
       https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/connectors/kafka.html#kafka-producer-partitioning-scheme */
      null,
      timestamp,
      key,
      value
    )
  }

  private def toJson(alarm: Alarm): String = {
    implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)
    write(alarm)
  }

}
