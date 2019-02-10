package com.github.tashoyan.telecom.predictor

import com.github.tashoyan.telecom.event.{DefaultEventDeduplicator, KafkaEventReceiver}
import org.apache.spark.sql.SparkSession

class SparkPredictorMain extends SparkPredictorArgParser {

  def main(args: Array[String]): Unit = {
    parser.parse(args, SparkPredictorConfig()) match {
      case Some(config) => doMain(config)
      case None => sys.exit(1)
    }
  }

  private def doMain(config: SparkPredictorConfig): Unit = {
    println(config)

    implicit val spark: SparkSession = SparkSession.builder()
      .getOrCreate()
    spark.sparkContext
      .setLogLevel("WARN")

    val eventReceiver = new KafkaEventReceiver(config.kafkaBrokers, config.kafkaEventTopic)
    val eventDeduplicator = new DefaultEventDeduplicator(config.watermarkIntervalSec)
    val kafkaEvents = eventReceiver.receiveEvents()
    val events = eventDeduplicator.deduplicateEvents(kafkaEvents)
    events.show()
    //TODO Complete
  }

}
