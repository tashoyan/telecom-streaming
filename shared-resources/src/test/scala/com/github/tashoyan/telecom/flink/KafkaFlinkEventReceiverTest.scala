package com.github.tashoyan.telecom.flink

import com.github.tashoyan.telecom.test.KafkaTestHarness
import net.manub.embeddedkafka.EmbeddedKafka
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.test.util.AbstractTestBase
import org.junit.{After, Before, Ignore, Test}
import org.scalatest.junit.JUnitSuiteLike

class KafkaFlinkEventReceiverTest extends AbstractTestBase with JUnitSuiteLike with KafkaTestHarness {

  @Before def beforeAll(): Unit = {
    startKafka()
  }

  @After def afterAll(): Unit = {
    stopKafka()
  }

  @Test def dummy0(): Unit = {}

  //TODO Make it working
  @Ignore def dummy(): Unit = {
    val kafkaBrokers = s"localhost:${embeddedKafkaConfig.kafkaPort}"
    val kafkaTopic = randomTopic("event")
    println(kafkaBrokers)
    println(kafkaTopic)

    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val eventReceiver = new KafkaFlinkEventReceiver(kafkaBrokers, kafkaTopic)
    val events = eventReceiver.receiveEvents()
    events.print()

    val streamThread = new Thread("stream") {
      override def run(): Unit = {
        env.execute()
        ()
      }
    }
    streamThread.start()

    EmbeddedKafka.publishStringMessageToKafka(kafkaTopic, "AAAAAA")
    EmbeddedKafka.publishStringMessageToKafka(kafkaTopic, "BBBBB")
    Thread.sleep(10000L)
    ()
  }

}
