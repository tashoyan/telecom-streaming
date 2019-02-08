package com.github.tashoyan.telecom.test

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

import scala.util.Random

trait KafkaTestHarness extends Suite with BeforeAndAfterAll with BeforeAndAfterEach {

  implicit var embeddedKafkaConfig: EmbeddedKafkaConfig = _

  override protected def beforeAll(): Unit = {
    val kafkaConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)
    embeddedKafkaConfig = EmbeddedKafka.start()(kafkaConfig).config
  }

  override protected def afterAll(): Unit = {
    EmbeddedKafka.stop()
  }

  override protected def beforeEach(): Unit = {
    //TODO Remove topics - is it possible?
  }

  protected def randomTopic(prefix: String): String =
    s"$prefix-${topicRandom.nextInt()}"

  private val topicRandom = new Random()

}
