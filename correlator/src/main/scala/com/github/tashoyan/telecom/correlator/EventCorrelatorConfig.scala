package com.github.tashoyan.telecom.correlator

case class EventCorrelatorConfig(
    topologyFile: String = "",
    kafkaBrokers: String = "",
    kafkaEventTopic: String = "",
    checkpointDir: String = "",
    kafkaAlarmTopic: String = "",
    watermarkIntervalMillis: Long = 0,
    windowSizeSec: Int = 0,
    windowShiftSec: Int = 0
)
