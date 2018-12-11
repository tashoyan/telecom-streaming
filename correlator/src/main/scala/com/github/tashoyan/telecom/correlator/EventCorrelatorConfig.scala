package com.github.tashoyan.telecom.correlator

case class EventCorrelatorConfig(
    schemaFile: String = "",
    topologyFile: String = "",
    kafkaBrokers: String = "",
    kafkaInputTopic: String = "",
    checkpointDir: String = "",
    kafkaOutputTopic: String = "",
    watermarkIntervalSec: Int = 0,
    windowSizeSec: Int = 0,
    windowShiftSec: Int = 0
)
