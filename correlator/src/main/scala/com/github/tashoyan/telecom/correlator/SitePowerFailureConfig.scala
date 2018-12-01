package com.github.tashoyan.telecom.correlator

import com.github.tashoyan.telecom.correlator.SitePowerFailureConfig._

case class SitePowerFailureConfig(
    schemaFile: String = "",
    kafkaBrokers: String = "",
    eventKafkaTopic: String = "",
    checkpointDir: String = defaultCheckpointDir,
    problemKafkaTopic: String = ""
)

object SitePowerFailureConfig {

  val defaultCheckpointDir: String = "checkpoint"

}

