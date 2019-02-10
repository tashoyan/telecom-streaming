package com.github.tashoyan.telecom.predictor

case class SparkPredictorConfig(
    kafkaBrokers: String = "",
    kafkaEventTopic: String = "",
    checkpointDir: String = "",
    kafkaAlarmTopic: String = "",
    watermarkIntervalSec: Int = 0
)
