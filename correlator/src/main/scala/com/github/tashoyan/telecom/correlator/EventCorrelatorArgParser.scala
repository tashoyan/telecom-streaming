package com.github.tashoyan.telecom.correlator

import scopt.OptionParser

trait EventCorrelatorArgParser {

  val parser: OptionParser[EventCorrelatorConfig] = new OptionParser[EventCorrelatorConfig]("event-correlator") {
    head("Event Correlator")

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

    opt[Long]("window-size-millis")
      .required()
      .valueName("<number>")
      .action((value, conf) => conf.copy(windowSizeMillis = value))
      .validate { value =>
        if (value <= 0) failure("Window size must be positive number")
        else success
      }
      .text("Sliding window size in milliseconds, used for time window aggregation")

    opt[Long]("window-slide-millis")
      .required()
      .valueName("<number>")
      .action((value, conf) => conf.copy(windowSlideMillis = value))
      .validate { value =>
        if (value <= 0) failure("Window slide must be positive number")
        else success
      }
      .text("Sliding window slide interval in milliseconds, used for time window aggregation")

    checkConfig { config =>
      if (config.windowSizeMillis < config.windowSlideMillis)
        failure("Window slide must be less or equal to window size")
      else
        success
    }

    help("help")
    version("version")
  }

}
