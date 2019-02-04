package com.github.tashoyan.telecom.event

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalatest.FunSuite

class KafkaEventLoaderTest extends FunSuite with EmbeddedKafka {

  test("dummy") {
    val kafkaConfig = EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)
    withRunningKafkaOnFoundPort(kafkaConfig) { implicit actualConfig =>
      publishStringMessageToKafka("topic", "msg")
      val msg = consumeFirstStringMessageFrom("topic")
      println(msg)
    }
  }

}
