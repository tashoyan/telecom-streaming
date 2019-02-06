package com.github.tashoyan.telecom.event

import java.nio.file.Files
import java.sql.Timestamp

import com.github.tashoyan.telecom.test.SparkTestHarness
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{Dataset, SaveMode}
import org.scalatest.FunSuite

import scala.util.Random

class KafkaEventLoaderTest extends FunSuite with EmbeddedKafka with SparkTestHarness {

  test("integration - writeEvents + loadEvents") {
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
      println(actualConfig)
      val kafkaBrokers = s"localhost:${actualConfig.kafkaPort}"
      val pollTimeoutMs = 500L
      val eventStream = new KafkaEventStream(
        kafkaBrokers,
        kafkaTopic = eventTopic(),
        partitionColumn = Event.siteIdColumn,
        checkpointDir = createCheckpointDir(),
        pollTimeoutMs = pollTimeoutMs
      )

      val eventsFromKafka = eventStream.loadEvents()
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
      val eventsToKafkaQuery = eventStream.writeEvents(eventsToKafka)

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
    resultEvents.show(false)
    val result = resultEvents.collect()
    assert(result.length === sample.length)
    //TODO Other checks
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
