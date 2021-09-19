package com.github.tashoyan.telecom.test

import io.github.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}

import scala.util.Random

trait KafkaTestHarness {

  implicit var embeddedKafkaConfig: EmbeddedKafkaConfig = _

  protected def startKafka(): Unit = {
    val kafkaConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)
    embeddedKafkaConfig = EmbeddedKafka.start()(kafkaConfig).config
  }

  protected def stopKafka(): Unit = {
    EmbeddedKafka.stop()
  }

  protected def randomTopic(prefix: String): String =
    s"$prefix-${topicRandom.nextInt()}"

  private val topicRandom = new Random()

}
