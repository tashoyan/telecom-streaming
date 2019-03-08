package com.github.tashoyan.telecom.writer

case class EventWriterConfig(
    kafkaBrokers: String = "",
    kafkaTopic: String = "",
    checkpointDir: String = "",
    outputDir: String = "",
    watermarkIntervalMillis: Long = 0
)
