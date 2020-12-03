package com.github.tashoyan.telecom.flink

import com.github.tashoyan.telecom.event.Event
import com.github.tashoyan.telecom.event.Event._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord

class EventDeserializationSchema extends KafkaDeserializationSchema[Event] {

  private val objectMapper = new ObjectMapper()

  override def isEndOfStream(nextElement: Event): Boolean = false

  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): Event = {
    val objectNode = objectMapper.readValue(record.value(), classOf[ObjectNode])
    Event(
      timestamp = objectNode.get(timestampColumn)
        .asLong(),
      siteId = objectNode.get(siteIdColumn)
        .asLong(),
      severity = objectNode.get(severityColumn)
        .asText(),
      info = objectNode.get(infoColumn)
        .asText()
    )
  }

  override def getProducedType: TypeInformation[Event] = {
    TypeInformation.of(classOf[Event])
  }

}
