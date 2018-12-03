package com.github.tashoyan.telecom.correlator

case class EventCorrelatorConfig(
    schemaFile: String = "",
    kafkaBrokers: String = "",
    kafkaTopic: String = "",
    checkpointDir: String = "",
    outputDir: String = ""
)
