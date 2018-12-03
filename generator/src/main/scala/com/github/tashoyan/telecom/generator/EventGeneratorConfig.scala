package com.github.tashoyan.telecom.generator

case class EventGeneratorConfig(
    schemaFile: String = "",
    inputDir: String = "",
    kafkaBrokers: String = "",
    kafkaTopic: String = "",
    checkpointDir: String = ""
)
