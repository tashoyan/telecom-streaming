package com.github.tashoyan.telecom.generator

import com.github.tashoyan.telecom.generator.EventGeneratorConfig._

case class EventGeneratorConfig(
    //    schemaFile: String = "",
    inputDir: String = "",
    kafkaBrokers: String = "",
    kafkaTopic: String = "",
    checkpointDir: String = defaultCheckpointDir
)

object EventGeneratorConfig {

  val defaultCheckpointDir: String = "checkpoint"

}
