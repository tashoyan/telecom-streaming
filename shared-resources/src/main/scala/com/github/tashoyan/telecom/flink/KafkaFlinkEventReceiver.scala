package com.github.tashoyan.telecom.flink

import java.util.Properties

import com.github.tashoyan.telecom.event.Event
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

class KafkaFlinkEventReceiver(
    kafkaBrokers: String,
    kafkaTopic: String
)(implicit env: StreamExecutionEnvironment) {

  //TODO How many consumers for a multi-partition topic?
  def receiveEvents(): DataStream[Event] = {
    val consumerProperties = new Properties()
    consumerProperties.setProperty("bootstrap.servers", kafkaBrokers)
    consumerProperties.setProperty("group.id", this.getClass.getSimpleName)

    val consumer = new FlinkKafkaConsumer[Event](kafkaTopic, new EventDeserializationSchema(), consumerProperties)
      .setStartFromGroupOffsets()

    env.addSource(consumer)
  }

}
