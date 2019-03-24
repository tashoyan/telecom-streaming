package com.github.tashoyan.telecom.flink

import java.util.Properties

import com.github.tashoyan.telecom.test.KafkaTestHarness
import net.manub.embeddedkafka.EmbeddedKafka
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.runtime.client.JobExecutionException
import org.apache.flink.streaming.api.scala.{DataStreamUtils, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.test.util.AbstractTestBase
import org.junit._
import org.scalatest.Matchers._
import org.scalatest.junit.JUnitSuiteLike

import scala.collection.mutable.ArrayBuffer

class KafkaFlinkEventReceiverTest extends AbstractTestBase with JUnitSuiteLike with KafkaTestHarness {

  @Before def beforeAll(): Unit = {
    startKafka()
  }

  @After def afterAll(): Unit = {
    stopKafka()
  }

  @Ignore def infinit(): Unit = {
    while (true) {
      println("---")
      Thread.sleep(1000L)
    }
  }

  @Ignore def justKafka(): Unit = {
    val kafkaBrokers = s"localhost:${embeddedKafkaConfig.kafkaPort}"
    val kafkaTopic = randomTopic("event")
    println(kafkaBrokers)
    println(kafkaTopic)

    val sendingEvents = Seq(
      "AAA",
      "BBB",
      "CCC"
    )
    sendingEvents.foreach { event =>
      EmbeddedKafka.publishStringMessageToKafka(kafkaTopic, event)
    }

    val receivedEvents = EmbeddedKafka.consumeNumberStringMessagesFrom(kafkaTopic, sendingEvents.length)
    receivedEvents.foreach(println)
  }

  @Ignore def flinkFixedWait(): Unit = {
    val kafkaBrokers = s"localhost:${embeddedKafkaConfig.kafkaPort}"
    val kafkaTopic = randomTopic("event")
    println(kafkaBrokers)
    println(kafkaTopic)

    val sendingEvents = Seq(
      "AAA",
      "BBB",
      "CCC"
    )

    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val consumerProperties = new Properties()
    consumerProperties.setProperty("bootstrap.servers", kafkaBrokers)
    val consumer = new FlinkKafkaConsumer[String](kafkaTopic, new SimpleStringSchema(), consumerProperties)
      .setStartFromEarliest()
    val receivedEvents = env.addSource(consumer)
    receivedEvents.print()
      .setParallelism(1)

    val streamThread = new Thread() {
      override def run(): Unit = {
        try {
          env.execute()
          ()
        } catch {
          case _: JobExecutionException =>
          /*The mini-cluster is stopped - ignore*/
        }
      }
    }
    streamThread.start()
    Thread.sleep(1000L)

    sendingEvents.foreach { event =>
      EmbeddedKafka.publishStringMessageToKafka(kafkaTopic, event)
    }
  }

  @Ignore def flinkWithSink(): Unit = {
    val kafkaBrokers = s"localhost:${embeddedKafkaConfig.kafkaPort}"
    val kafkaTopic = randomTopic("event")
    println(kafkaBrokers)
    println(kafkaTopic)

    val sendingEvents = Seq(
      "AAA",
      "BBB",
      "CCC"
    )

    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val consumerProperties = new Properties()
    consumerProperties.setProperty("bootstrap.servers", kafkaBrokers)
    val consumer = new FlinkKafkaConsumer[String](kafkaTopic, new SimpleStringSchema(), consumerProperties)
      .setStartFromEarliest()
    val receivedEvents = env.addSource(consumer)
    receivedEvents.print()
      .setParallelism(1)

    val receivedEventsIter = new DataStreamUtils[String](receivedEvents)
      .collect()
    Thread.sleep(1000L)

    sendingEvents.foreach { event =>
      EmbeddedKafka.publishStringMessageToKafka(kafkaTopic, event)
    }

    var receivedCount = 0
    while (receivedCount < sendingEvents.length && receivedEventsIter.hasNext) {
      println(s"Received: ${receivedEventsIter.next()}")
      receivedCount += 1
    }
  }

  @Test(timeout = 10000L) def flinkCheckSinkStartedToReceive(): Unit = {
    val kafkaBrokers = s"localhost:${embeddedKafkaConfig.kafkaPort}"
    val kafkaTopic = randomTopic("event")
    println(kafkaBrokers)
    println(kafkaTopic)

    val sendingEvents = Seq(
      "AAA",
      "BBB",
      "CCC"
    )

    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val consumerProperties = new Properties()
    consumerProperties.setProperty("bootstrap.servers", kafkaBrokers)
    val consumer = new FlinkKafkaConsumer[String](kafkaTopic, new SimpleStringSchema(), consumerProperties)
      .setStartFromEarliest()
    val eventStream = env.addSource(consumer)
    eventStream.print()
      .setParallelism(1)

    val receivedEventsIter = new DataStreamUtils[String](eventStream)
      .collect()

    val testMsg = "test"
    object TestMsgThread extends Thread() {
      @volatile var isActive = true

      override def run(): Unit = {
        while (isActive) {
          EmbeddedKafka.publishStringMessageToKafka(kafkaTopic, testMsg)
          Thread.sleep(500L)
        }
      }
    }
    TestMsgThread.start()

    if (receivedEventsIter.hasNext && receivedEventsIter.next() == testMsg) {
      TestMsgThread.isActive = false
      sendingEvents.foreach { event =>
        EmbeddedKafka.publishStringMessageToKafka(kafkaTopic, event)
      }
      val receivedEvents = new ArrayBuffer[String](sendingEvents.length)
      while (receivedEvents.length < sendingEvents.length && receivedEventsIter.hasNext) {
        val e = receivedEventsIter.next()
        if (e != testMsg) {
          println(s"Received: $e")
          receivedEvents += e
        }
      }
      receivedEvents should be(sendingEvents)
    }
    TestMsgThread.join()
  }

}
