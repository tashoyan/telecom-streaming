package com.github.tashoyan.telecom.flink

import com.github.tashoyan.telecom.event.Event
import com.github.tashoyan.telecom.test.KafkaTestHarness
import net.manub.embeddedkafka.EmbeddedKafka
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.{DataStreamUtils, StreamExecutionEnvironment, _}
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

  @Test(timeout = 10000L) def receiveEvents(): Unit = {
    val kafkaBrokers = s"localhost:${embeddedKafkaConfig.kafkaPort}"
    val kafkaTopic = randomTopic("event")
    val sendingEvents = Seq(
      Event(1L, 100L, "major", "AAA"),
      Event(2L, 200L, "major", "BBB"),
      Event(3L, 300L, "major", "CCC")
    )

    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val eventReceiver = new KafkaFlinkEventReceiver(kafkaBrokers, kafkaTopic)
    val eventStream = eventReceiver.receiveEvents()

    val receivedEventsIter = setupReceive(kafkaTopic, eventStream, testEvent, toJson)

    sendingEvents.foreach { event =>
      EmbeddedKafka.publishStringMessageToKafka(kafkaTopic, toJson(event))
    }

    val receivedEvents = receivedEventsIter
      .take(sendingEvents.length)
      .toSeq
    receivedEvents should be(sendingEvents)
    println(s"Received: $receivedEvents")
    ()
  }

  private def toJson(event: Event): String = {
    import org.json4s._
    import org.json4s.native.Serialization
    import org.json4s.native.Serialization.write

    implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)
    write(event)
  }

  private val testEvent = Event(0L, 0L, "", "")

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
    * @param kafkaTopic      The topic the data stream consumes from.
    * @param stream          Data stream.
    * @param testMessage     Special message to test that the stream started to receive messages.
    *                        This function sends test messages to Kafka until the stream received at least one.
    * @param messageToString Function to serialize a message to String.
    *                        Then, StringSerializer is used to serialize messages to bytes.
    * @tparam T Data type of messages.
    * @return Iterator with blocking `hasNext()` `next()` functions.
    *         These functions block until the data is available.
    *         All the messages sent to the Kafka topic after this iterator is created, will be eventually available in this iterator.
    *         The message order is preserved.
    */
  //TODO Extract to harness
  private def setupReceive[T: TypeInformation: ClassTag](kafkaTopic: String, stream: DataStream[T], testMessage: T, messageToString: T => String): Iterator[T] = {
    val receiveIter = new DataStreamUtils[T](stream)
      .collect()

    object SendTestMessages extends Thread(this.getClass.getSimpleName + "-SendTestMessages") {
      @volatile var isActive = true

      override def run(): Unit = {
        while (isActive) {
          EmbeddedKafka.publishStringMessageToKafka(kafkaTopic, messageToString(testMessage))
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
