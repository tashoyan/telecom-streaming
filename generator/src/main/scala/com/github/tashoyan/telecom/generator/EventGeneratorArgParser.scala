package com.github.tashoyan.telecom.generator

import com.github.tashoyan.telecom.generator.EventGeneratorConfig._
import scopt.OptionParser

trait EventGeneratorArgParser {

  val parser: OptionParser[EventGeneratorConfig] = new OptionParser[EventGeneratorConfig]("event-producer") {
    head("Event Producer")

    opt[String]("schema-file")
      .required()
      .valueName("<path>")
      .action((value, conf) => conf.copy(schemaFile = value))
      .validate { value =>
        if (value.isEmpty) failure("Path to events schema file must not be empty string")
        else success
      }
      .text("Full path to the Parquet location having event's schema")

    opt[String]("input-dir")
      .required()
      .valueName("<path>")
      .action((value, conf) => conf.copy(inputDir = value))
      .validate { value =>
        if (value.isEmpty) failure("Input events path must not be empty string")
        else success
      }
      .text("Full path to the input directory where Parquet files with events are being added")

    opt[String]("kafka-brokers")
      .required()
      .valueName("host1:9092[,host2:9092,...]")
      .action((value, conf) => conf.copy(kafkaBrokers = value))
      .validate { value =>
        if (value.isEmpty) failure("Kafka brokers list must not be empty string")
        else success
      }
      .text("List of comma-separated Kafka brokers")

    opt[String]("kafka-topic")
      .required()
      .valueName("<topic>")
      .action((value, conf) => conf.copy(kafkaTopic = value))
      .validate { value =>
        if (value.isEmpty) failure("Kafka topic must not be empty string")
        else success
      }
      .text("Kafka topic to send events to")

    opt[String]("checkpoint-dir")
      .optional()
      .valueName("<path>")
      .action((value, conf) => conf.copy(checkpointDir = value))
      .validate { value =>
        if (value.isEmpty) failure("Checkpoint directory must not be empty string")
        else success
      }
      .text(s"Checkpoint directory on HDFS. Default is $defaultCheckpointDir.")

    help("help")
    version("version")
  }

}
