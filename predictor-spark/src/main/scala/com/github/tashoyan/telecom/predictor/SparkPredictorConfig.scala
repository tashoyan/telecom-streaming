package com.github.tashoyan.telecom.predictor

case class SparkPredictorConfig(
    kafkaBrokers: String = "",
    kafkaEventTopic: String = "",
    checkpointDir: String = "",
    kafkaAlarmTopic: String = "",
    watermarkIntervalMillis: Long = 0,
    problemTimeoutMillis: Long = 0
)
