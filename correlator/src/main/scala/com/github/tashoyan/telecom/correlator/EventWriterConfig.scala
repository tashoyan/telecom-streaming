package com.github.tashoyan.telecom.correlator

import com.github.tashoyan.telecom.correlator.EventWriterConfig._

case class EventWriterConfig(
    schemaFile: String = "",
    kafkaBrokers: String = "",
    kafkaTopic: String = "",
    checkpointDir: String = defaultCheckpointDir,
    outputDir: String = ""
)

object EventWriterConfig {

  val defaultCheckpointDir: String = "checkpoint"

}
