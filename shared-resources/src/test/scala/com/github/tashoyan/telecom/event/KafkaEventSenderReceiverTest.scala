package com.github.tashoyan.telecom.event

import java.nio.file.Files
import java.util.concurrent.TimeoutException

import com.github.tashoyan.telecom.test.{KafkaTestHarness, SparkTestHarness}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.{Dataset, SaveMode}
import org.scalatest.FunSuite

class KafkaEventSenderReceiverTest extends FunSuite with KafkaTestHarness with SparkTestHarness {

  test("integration - send + receive events") {
    val spark0 = spark
    import spark0.implicits._

    val sample = Seq(
      Event(1L, siteId = 0L, severity = "MAJOR", info = "event1"),
      Event(2L, siteId = 0L, severity = "MAJOR", info = "event2"),
      Event(3L, siteId = 0L, severity = "MAJOR", info = "event3")
    )
    val eventSample: Dataset[Event] = sample.toDS()

    val eventInputDir = createInputDir()
    val eventOutputDir = createOutputDir()

    val kafkaBrokers = s"localhost:${embeddedKafkaConfig.kafkaPort}"
    val kafkaTopic = randomTopic("event")
    val eventReceiver = new KafkaEventReceiver(
      kafkaBrokers,
      kafkaTopic
    )
    val eventSender = new KafkaStreamingSender[Event](
      kafkaBrokers,
      kafkaTopic,
      partitionColumn = Event.siteIdColumn,
      checkpointDir = createCheckpointDir(),
      OutputMode.Append()
    )

    val eventsFromKafka = eventReceiver.receiveEvents()
    val eventsFromKafkaQuery = eventsFromKafka
      .map(_.toEvent)
      .writeStream
      .outputMode(OutputMode.Append())
      .format("parquet")
      .option("path", eventOutputDir)
      .option("checkpointLocation", createCheckpointDir())
      .start()

    /*Make sure the consumer will get the second batch from Kafka*/
    skipOneTrigger(eventsFromKafkaQuery)

    val eventsToKafka = spark.readStream
      .schema(eventSample.schema)
      .parquet(eventInputDir)
      .as[Event]
    val eventsToKafkaQuery = eventSender.sendingQuery(eventsToKafka)

    eventSample
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(eventInputDir)

    eventsToKafkaQuery.processAllAvailable()
    eventsFromKafkaQuery.processAllAvailable()

    val resultEvents = spark.read
      .parquet(eventOutputDir)
      .as[Event]
    val result = resultEvents.collect()
    assert(result.length === sample.length)
    assert(result.sortBy(_.timestamp) === sample.sortBy(_.timestamp))
  }

  private def createCheckpointDir(): String =
    createTempDir("checkpoint")

  private def createInputDir(): String =
    createTempDir("event-input")

  private def createOutputDir(): String =
    createTempDir("event-output")

  private def createTempDir(prefix: String): String = {
    Files.createTempDirectory(s"$prefix-")
      .toAbsolutePath
      .toString
  }

  private def skipOneTrigger(streamingQuery: StreamingQuery): Unit = {
    val recheckTimeoutMs = 1000L
    val awaitTimeoutMs = 10000L
    waitFor(
      awaitTimeoutMs,
      recheckTimeoutMs,
      () => streamingQuery.status.isTriggerActive,
      () => streamingQuery.status.toString()
    )
    waitFor(
      awaitTimeoutMs,
      recheckTimeoutMs,
      () => !streamingQuery.status.isTriggerActive,
      () => streamingQuery.status.toString()
    )
  }

  //TODO Extract this generic function to test-resources
  private def waitFor(
      awaitTimeoutMs: Long,
      recheckTimeoutMs: Long,
      condition: () => Boolean,
      timeoutStatus: () => String
  ): Unit = {
    val t0 = System.currentTimeMillis()
    while (!condition()) {
      val t = System.currentTimeMillis()
      if (t - t0 > awaitTimeoutMs) {
        throw new TimeoutException(s"Timed out $awaitTimeoutMs ms; status: ${timeoutStatus()}")
      }
      Thread.sleep(recheckTimeoutMs)
    }
  }

}
