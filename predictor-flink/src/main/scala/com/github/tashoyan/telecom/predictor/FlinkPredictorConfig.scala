package com.github.tashoyan.telecom.predictor

/*
TODO Align settings for Spark and Flink implementation.
 Ideally should be same and don't contain Spark/Flink specific settings.
*/
case class FlinkPredictorConfig(
    kafkaBrokers: String = "",
    kafkaEventTopic: String = "",
    checkpointDir: String = "",
    kafkaAlarmTopic: String = "",
    watermarkIntervalMillis: Long = 0,
    problemTimeoutMillis: Long = 0
)
