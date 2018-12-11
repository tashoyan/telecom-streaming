package com.github.tashoyan.telecom.correlator

import scopt.OptionParser

trait EventCorrelatorArgParser {

  val parser: OptionParser[EventCorrelatorConfig] = new OptionParser[EventCorrelatorConfig]("event-correlator") {
    head("Event Writer")

    opt[String]("schema-file")
      .required()
      .valueName("<path>")
      .action((value, conf) => conf.copy(schemaFile = value))
      .validate { value =>
        if (value.isEmpty) failure("Path to events schema file must not be empty string")
        else success
      }
      .text("Full path to the Parquet location having event's schema")

    opt[String]("topology-file")
      .required()
      .valueName("<path>")
      .action((value, conf) => conf.copy(topologyFile = value))
      .validate { value =>
        if (value.isEmpty) failure("Path to topology file must not be empty string")
        else success
      }
      .text("Full path to the Parquet location with topology file")

    opt[String]("kafka-brokers")
      .required()
      .valueName("host1:9092[,host2:9092,...]")
      .action((value, conf) => conf.copy(kafkaBrokers = value))
      .validate { value =>
        if (value.isEmpty) failure("Kafka brokers list must not be empty string")
        else success
      }
      .text("List of comma-separated Kafka brokers")

    opt[String]("kafka-input-topic")
      .required()
      .valueName("<topic>")
      .action((value, conf) => conf.copy(kafkaInputTopic = value))
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

    opt[String]("kafka-output-topic")
      .required()
      .valueName("<topic>")
      .action((value, conf) => conf.copy(kafkaOutputTopic = value))
      .validate { value =>
        if (value.isEmpty) failure("Kafka topic must not be empty string")
        else success
      }
      .text("Kafka topic to send events to")

    opt[Int]("watermark-interval-sec")
      .required()
      .valueName("<number>")
      .action((value, conf) => conf.copy(watermarkIntervalSec = value))
      .validate { value =>
        if (value <= 0) failure("Watermark interval must be positive number")
        else success
      }
      .text("Watermark interval in seconds, used for time window aggregation and for event deduplication")

    help("help")
    version("version")
  }

}
