package com.github.tashoyan.telecom.flink

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

class KafkaFlinkEventReceiver(
    kafkaBrokers: String,
    kafkaTopic: String
)(implicit env: StreamExecutionEnvironment) {

  //TODO Should be stream of Event
  def receiveEvents(): DataStream[String] = {
    val consumerProperties = new Properties()
    consumerProperties.setProperty("bootstrap.servers", kafkaBrokers)
    consumerProperties.setProperty("group.id", this.getClass.getSimpleName)

    val consumer = new FlinkKafkaConsumer[String](kafkaTopic, new SimpleStringSchema(), consumerProperties)
      .setStartFromGroupOffsets()

    env.addSource(consumer)
  }

}
