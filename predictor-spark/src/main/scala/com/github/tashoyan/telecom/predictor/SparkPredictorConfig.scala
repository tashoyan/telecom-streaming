package com.github.tashoyan.telecom.predictor

case class SparkPredictorConfig(
    schemaFile: String = "",
    kafkaBrokers: String = "",
    kafkaInputTopic: String = ""
)
