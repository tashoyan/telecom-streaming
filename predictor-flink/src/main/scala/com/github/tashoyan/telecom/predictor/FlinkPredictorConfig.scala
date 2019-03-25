package com.github.tashoyan.telecom.predictor

/*
TODO Align settings for Spark and Flink implementation.
 Ideally should be same and don't contain Spark/Flink specific settings.
*/
case class FlinkPredictorConfig(
    kafkaBrokers: String = "",
    kafkaEventTopic: String = "",
    kafkaAlarmTopic: String = "",
    problemTimeoutMillis: Long = 0
)
