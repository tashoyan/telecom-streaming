package com.github.tashoyan.telecom.flink

import java.util.concurrent.CountDownLatch

import com.github.tashoyan.telecom.test.KafkaTestHarness
import net.manub.embeddedkafka.EmbeddedKafka
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
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

  @Ignore def dummy0(): Unit = {}

  //TODO Make it working
  @Test def dummy(): Unit = {
    val kafkaBrokers = s"localhost:${embeddedKafkaConfig.kafkaPort}"
    val kafkaTopic = randomTopic("event")
    println(kafkaBrokers)
    println(kafkaTopic)

    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val eventReceiver = new KafkaFlinkEventReceiver(kafkaBrokers, kafkaTopic)
    val events = eventReceiver.receiveEvents()

    val latch = new CountDownLatch(1)
    val streamThread = new Thread("stream") {
      override def run(): Unit = {
        latch.countDown()
        val output = new DataStreamUtils[String](events).collect().toList
        println(s"Output: $output")
        ()
      }
    }
    streamThread.start()

    latch.await()
    Thread.sleep(10000L)
    println("Sending to Kafka")
    EmbeddedKafka.publishStringMessageToKafka(kafkaTopic, "AAAAAA")
    EmbeddedKafka.publishStringMessageToKafka(kafkaTopic, "BBBBB")
    Thread.sleep(10000L)
    ()
  }

}
