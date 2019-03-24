package com.github.tashoyan.telecom.flink

import java.util.Properties

import com.github.tashoyan.telecom.test.KafkaTestHarness
import net.manub.embeddedkafka.EmbeddedKafka
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.runtime.client.JobExecutionException
import org.apache.flink.streaming.api.scala.{DataStreamUtils, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.test.util.AbstractTestBase
import org.junit._
import org.scalatest.Matchers._
import org.scalatest.junit.JUnitSuiteLike

import scala.reflect.ClassTag

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

  @Ignore def flinkCheckSinkStartedToReceive(): Unit = {
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

    val receivedEventsIter = setupReceive(kafkaTopic, eventStream, "test")

    sendingEvents.foreach { event =>
      EmbeddedKafka.publishStringMessageToKafka(kafkaTopic, event)
    }

    val receivedEvents = receivedEventsIter
      .take(sendingEvents.length)
      .toSeq

    receivedEvents should be(sendingEvents)
    println(s"Received: $receivedEvents")
  }

  @Test(timeout = 10000L) def receiveEvents(): Unit = {
    val kafkaBrokers = s"localhost:${embeddedKafkaConfig.kafkaPort}"
    val kafkaTopic = randomTopic("event")
    val sendingEvents = Seq(
      "AAA",
      "BBB",
      "CCC"
    )

    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val eventReceiver = new KafkaFlinkEventReceiver(kafkaBrokers, kafkaTopic)
    val eventStream = eventReceiver.receiveEvents()

    val receivedEventsIter = setupReceive(kafkaTopic, eventStream, "test")

    sendingEvents.foreach { event =>
      EmbeddedKafka.publishStringMessageToKafka(kafkaTopic, event)
    }

    val receivedEvents = receivedEventsIter
      .take(sendingEvents.length)
      .toSeq
    receivedEvents should be(sendingEvents)
    ()
  }

  /**
    * Sets up the data stream to receive messages sent to a Kafka topic.
    *
    * This function provides the happens-before relationship:
    * all messages sent to the Kafka topic after the this function is called
    * will be eventually available in the returned iterator.
    * This function is needed for a data stream that consumes messages from Kafka starting from the latest offsets.
    * There is a risk that a message gets to the Kafka topic before the stream started to consume,
    * so this message will not be consumed by the stream.
    *
    * @param kafkaTopic  The topic the data stream consumes from.
    * @param stream      Data stream.
    * @param testMessage Special message to test that the stream started to receive messages.
    *                    This function sends test messages to Kafka until the stream received at least one.
    * @tparam T Data type of messages.
    * @return Iterator with blocking `hasNext()` `next()` functions.
    *         These functions block until the data is available.
    *         All the messages sent to the Kafka topic after this iterator is created, will be eventually available in this iterator.
    *         The message order is preserved.
    */
  //TODO Extract to harness
  private def setupReceive[T: TypeInformation: ClassTag](kafkaTopic: String, stream: DataStream[T], testMessage: T): Iterator[T] = {
    val receiveIter = new DataStreamUtils[T](stream)
      .collect()

    object SendTestMessages extends Thread(this.getClass.getSimpleName + "-SendTestMessages") {
      @volatile var isActive = true

      override def run(): Unit = {
        while (isActive) {
          //TODO Serializer
          EmbeddedKafka.publishStringMessageToKafka(kafkaTopic, testMessage.toString)
          Thread.sleep(500L)
        }
      }
    }
    SendTestMessages.start()

    try {
      if (receiveIter.contains(testMessage)) {
        SendTestMessages.isActive = false
        receiveIter.filterNot(_ == testMessage)
      } else {
        throw new RuntimeException(s"Failed to setup a stream consuming from Kafka topic $kafkaTopic - failed to receive test message $testMessage")
      }
    } finally {
      SendTestMessages.join()
    }
  }

}
