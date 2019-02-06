package com.github.tashoyan.telecom.event

import java.io.File
import java.sql.Timestamp

import com.github.tashoyan.telecom.test.SparkTestHarness
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{Dataset, SaveMode}
import org.scalatest.FunSuite

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

    //TODO Inject "target" via Maven property
    //TODO Temporary file to avoid tests interference
    val eventInputDir = "target/event-input"
    val eventOutputDir = "target/event-output"
    new File(eventInputDir).mkdirs()

    val kafkaConfig = EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)
    //TODO Set up / tear down once for faster tests run
    withRunningKafkaOnFoundPort(kafkaConfig) { implicit actualConfig =>
      println(actualConfig)
      val kafkaBrokers = s"localhost:${actualConfig.kafkaPort}"
      //TODO Random topic to avoid tests interference
      val kafkaTopic = "events"
      //TODO Inject "target" via Maven property
      //TODO Temporary file to avoid tests interference
      val checkpointDir = "target/checkpoint"
      val pollTimeoutMs = 500L
      val eventStream = new KafkaEventStream(
        kafkaBrokers,
        kafkaTopic,
        Event.siteIdColumn,
        checkpointDir,
        pollTimeoutMs
      )

      val eventsFromKafka = eventStream.loadEvents()
      val eventsFromKafkaQuery = eventsFromKafka.writeStream
        .outputMode(OutputMode.Append())
        .format("parquet")
        .option("path", eventOutputDir)
        //TODO Inject "target" via Maven property
        //TODO Temporary file to avoid tests interference
        .option("checkpointLocation", "target/checkpoint1")
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
        //TODO Temporary file to avoid tests interference
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

  //TODO Remove
  ignore("dummy") {
    val kafkaConfig = EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)
    withRunningKafkaOnFoundPort(kafkaConfig) { implicit actualConfig =>
      publishStringMessageToKafka("topic", "msg")
      val msg = consumeFirstStringMessageFrom("topic")
      println(msg)
    }
  }

}
