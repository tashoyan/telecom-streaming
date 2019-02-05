package com.github.tashoyan.telecom.event

import java.sql.Timestamp
import java.util.concurrent.TimeUnit

import com.github.tashoyan.telecom.test.SparkTestHarness
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.streaming.OutputMode
import org.scalatest.FunSuite

class KafkaEventLoaderTest extends FunSuite with EmbeddedKafka with SparkTestHarness {

  test("join") {
    val spark0 = spark
    import spark0.implicits._

    val events: Dataset[Event] = Seq(
      Event(new Timestamp(1L), siteId = 0L, severity = "MAJOR", info = "event1"),
      Event(new Timestamp(2L), siteId = 0L, severity = "MAJOR", info = "event2"),
      Event(new Timestamp(3L), siteId = 0L, severity = "MAJOR", info = "event3")
    ).toDS()

    val rates: Dataset[(Long, String)] = Seq(
      (0L, "rate1"),
      (0L, "rate2")
    ).toDF("siteId", "msg")
      .as[(Long, String)]

    val res = events.join(rates, Seq("siteId"), "inner")
    res.show(false)
  }

  ignore("dummy") {
    val kafkaConfig = EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)
    withRunningKafkaOnFoundPort(kafkaConfig) { implicit actualConfig =>
      publishStringMessageToKafka("topic", "msg")
      val msg = consumeFirstStringMessageFrom("topic")
      println(msg)
    }
  }

  ignore("event stream") {
    val spark0 = spark
    import spark0.implicits._

    val kafkaConfig = EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)
    withRunningKafkaOnFoundPort(kafkaConfig) { implicit actualConfig =>
      println(actualConfig)

      val events: Dataset[Event] = Seq(
        Event(new Timestamp(1L), siteId = 0L, severity = "MAJOR", info = "event1"),
        Event(new Timestamp(2L), siteId = 0L, severity = "MAJOR", info = "event2"),
        Event(new Timestamp(3L), siteId = 0L, severity = "MAJOR", info = "event3")
      ).toDS()

      //      val events = spark.readStream
      //        .format("rate")
      //        .option("rowsPerSecond", 1)
      //        .load()

      val query = events.writeStream
        .outputMode(OutputMode.Append())
        .queryName(this.getClass.getSimpleName)
        .format("console")
        .option("truncate", value = false)
        .start()
      query.awaitTermination(TimeUnit.SECONDS.toMillis(10))
    }
  }

}
