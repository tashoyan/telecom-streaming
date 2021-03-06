package com.github.tashoyan.telecom.predictor

import scopt.OptionParser

//TODO Extract common options to a super-trait
trait SparkPredictorArgParser {

  val parser: OptionParser[SparkPredictorConfig] = new OptionParser[SparkPredictorConfig]("predictor") {
    head("Predictor")

    opt[String]("kafka-brokers")
      .required()
      .valueName("host1:9092[,host2:9092,...]")
      .action((value, conf) => conf.copy(kafkaBrokers = value))
      .validate { value =>
        if (value.isEmpty) failure("Kafka brokers list must not be empty string")
        else success
      }
      .text("List of comma-separated Kafka brokers")

    opt[String]("kafka-event-topic")
      .required()
      .valueName("<topic>")
      .action((value, conf) => conf.copy(kafkaEventTopic = value))
      .validate { value =>
        if (value.isEmpty) failure("Kafka topic must not be empty string")
        else success
      }
      .text("Kafka topic to receive events from")

    opt[String]("checkpoint-dir")
      .required()
      .valueName("<path>")
      .action((value, conf) => conf.copy(checkpointDir = value))
      .validate { value =>
        if (value.isEmpty) failure("Checkpoint directory must not be empty string")
        else success
      }
      .text(s"Checkpoint directory on HDFS.")

    opt[String]("kafka-alarm-topic")
      .required()
      .valueName("<topic>")
      .action((value, conf) => conf.copy(kafkaAlarmTopic = value))
      .validate { value =>
        if (value.isEmpty) failure("Kafka topic must not be empty string")
        else success
      }
      .text("Kafka topic to send events to")

    opt[Long]("watermark-interval-millis")
      .required()
      .valueName("<number>")
      .action((value, conf) => conf.copy(watermarkIntervalMillis = value))
      .validate { value =>
        if (value <= 0) failure("Watermark interval must be positive number")
        else success
      }
      .text("Watermark interval in milliseconds, used for time window aggregation and for event deduplication")

    opt[Long]("problem-timeout-millis")
      .required()
      .valueName("<number>")
      .action((value, conf) => conf.copy(problemTimeoutMillis = value))
      .validate { value =>
        if (value <= 0) failure("Problem timeout must be positive number")
        else success
      }
      .text("Problem timeout in milliseconds. If triggering events fit this time interval, then an alarm will be created.")

    help("help")
    version("version")
  }

}
