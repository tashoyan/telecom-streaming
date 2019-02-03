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

    opt[String]("kafka-input-topic")
      .required()
      .valueName("<topic>")
      .action((value, conf) => conf.copy(kafkaInputTopic = value))
      .validate { value =>
        if (value.isEmpty) failure("Kafka topic must not be empty string")
        else success
      }
      .text("Kafka topic to receive events from")

    help("help")
    version("version")
  }

}
