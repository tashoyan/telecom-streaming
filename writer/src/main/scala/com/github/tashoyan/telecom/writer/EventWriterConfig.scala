package com.github.tashoyan.telecom.writer

case class EventWriterConfig(
    kafkaBrokers: String = "",
    kafkaTopic: String = "",
    checkpointDir: String = "",
    outputDir: String = "",
    watermarkIntervalSec: Int = 0
)
