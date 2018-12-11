package com.github.tashoyan.telecom.writer

case class EventWriterConfig(
    schemaFile: String = "",
    kafkaBrokers: String = "",
    kafkaTopic: String = "",
    checkpointDir: String = "",
    outputDir: String = "",
    watermarkIntervalSec: Int = 0
)
