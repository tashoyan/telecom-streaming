package com.github.tashoyan.telecom.correlator

case class EventCorrelatorConfig(
    topologyFile: String = "",
    kafkaBrokers: String = "",
    kafkaEventTopic: String = "",
    checkpointDir: String = "",
    kafkaAlarmTopic: String = "",
    watermarkIntervalMillis: Long = 0,
    windowSizeMillis: Long = 0,
    windowSlideMillis: Long = 0
)
