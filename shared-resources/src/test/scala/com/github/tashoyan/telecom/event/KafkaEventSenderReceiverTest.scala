package com.github.tashoyan.telecom.event

import java.nio.file.Files
import java.sql.Timestamp

import com.github.tashoyan.telecom.spark.KafkaStream._
import com.github.tashoyan.telecom.test.SparkTestHarness
import com.github.tashoyan.telecom.util.Timestamps.RichTimestamp
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{Dataset, SaveMode}
import org.scalatest.FunSuite

import scala.util.Random

class KafkaEventSenderReceiverTest extends FunSuite with EmbeddedKafka with SparkTestHarness {

  test("integration - send + receive events") {
    val spark0 = spark
    import spark0.implicits._

    val sample = Seq(
      Event(new Timestamp(1L), siteId = 0L, severity = "MAJOR", info = "event1"),
      Event(new Timestamp(2L), siteId = 0L, severity = "MAJOR", info = "event2"),
      Event(new Timestamp(3L), siteId = 0L, severity = "MAJOR", info = "event3")
    )
    val eventSample: Dataset[Event] = sample.toDS()

    val eventInputDir = createInputDir()
    val eventOutputDir = createOutputDir()

    val kafkaConfig = EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)
    //TODO Set up / tear down once for faster tests run
    withRunningKafkaOnFoundPort(kafkaConfig) { implicit actualConfig =>
      val kafkaBrokers = s"localhost:${actualConfig.kafkaPort}"
      val kafkaTopic = eventTopic()
      val pollTimeoutMs = defaultPollTimeoutMs
      val eventReceiver = new KafkaEventReceiver(
        kafkaBrokers,
        kafkaTopic,
        pollTimeoutMs
      )
      val eventSender = new KafkaEventSender(
        kafkaBrokers,
        kafkaTopic,
        partitionColumn = Event.siteIdColumn,
        checkpointDir = createCheckpointDir()
      )

      val eventsFromKafka = eventReceiver.receiveEvents()
      val eventsFromKafkaQuery = eventsFromKafka.writeStream
        .outputMode(OutputMode.Append())
        .format("parquet")
        .option("path", eventOutputDir)
        .option("checkpointLocation", createCheckpointDir())
        .start()

      val eventsToKafka = spark.readStream
        .schema(eventSample.schema)
        .parquet(eventInputDir)
        .as[Event]
      val eventsToKafkaQuery = eventSender.sendEvents(eventsToKafka)

      /*Make sure the consumer gets the second batch from Kafka*/
      Thread.sleep(pollTimeoutMs)
      eventSample
        .repartition(1)
        .write
        .mode(SaveMode.Overwrite)
        .parquet(eventInputDir)

      eventsToKafkaQuery.processAllAvailable()
      eventsFromKafkaQuery.processAllAvailable()
    }

    val resultEvents = spark.read
      .parquet(eventOutputDir)
      .as[Event]
    val result = resultEvents.collect()
    assert(result.length === sample.length)
    assert(result.sortBy(_.timestamp) === sample.sortBy(_.timestamp))
  }

  private def createCheckpointDir(): String =
    createTempDir("checkpoint-")

  private def createInputDir(): String =
    createTempDir("event-input-")

  private def createOutputDir(): String =
    createTempDir("event-output-")

  private def createTempDir(prefix: String): String = {
    Files.createTempDirectory(prefix)
      .toAbsolutePath
      .toString
  }

  private def eventTopic(): String =
    s"events-${topicRandom.nextInt()}"

  private val topicRandom = new Random()

}
