package com.github.tashoyan.telecom.flink

import java.lang.{Long => JLong}
import java.nio.charset.StandardCharsets

import com.github.tashoyan.telecom.event.{Alarm, AlarmJson}
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema
import org.apache.kafka.clients.producer.ProducerRecord

class AlarmSerializationSchema(kafkaTopic: String) extends KafkaSerializationSchema[Alarm] {

  override def serialize(alarm: Alarm, timestamp: JLong): ProducerRecord[Array[Byte], Array[Byte]] = {
    /* All alarms having same objectId go to the same Kafka partition */
    val key = Array(alarm.objectId.toByte)
    val value = AlarmJson.toJson(alarm)
      .getBytes(StandardCharsets.UTF_8)
    new ProducerRecord[Array[Byte], Array[Byte]](
      kafkaTopic,
      /* Assign topic partitions according to message keys:
       https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/connectors/kafka.html#kafka-producer-partitioning-scheme */
      //scalastyle:off
      null,
      //scalastyle:on
      timestamp,
      key,
      value
    )
  }

}
