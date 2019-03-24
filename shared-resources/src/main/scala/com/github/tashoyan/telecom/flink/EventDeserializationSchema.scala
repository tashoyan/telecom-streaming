package com.github.tashoyan.telecom.flink

import com.github.tashoyan.telecom.event.Event
import com.github.tashoyan.telecom.event.Event._
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema
import org.apache.flink.formats.json.JsonNodeDeserializationSchema

class EventDeserializationSchema extends AbstractDeserializationSchema[Event] {
  private val delegate = new JsonNodeDeserializationSchema

  override def deserialize(message: Array[Byte]): Event = {
    val objectNode = delegate.deserialize(message)
    Event(
      timestamp = objectNode.get(timestampColumn).asLong(),
      siteId = objectNode.get(siteIdColumn).asLong(),
      severity = objectNode.get(severityColumn).asText(),
      info = objectNode.get(infoColumn).asText()
    )
  }

}
