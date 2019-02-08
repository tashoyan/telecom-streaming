package com.github.tashoyan.telecom.predictor

case class SparkPredictorConfig(
    kafkaBrokers: String = "",
    kafkaInputTopic: String = "",
    checkpointDir: String = "",
    kafkaOutputTopic: String = "",
    watermarkIntervalSec: Int = 0
)
