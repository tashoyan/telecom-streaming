package com.github.tashoyan.telecom.spark

object SparkKafkaStream {

  /**
    * Data set column used for keys when reading/writing data from/to Kafka topics.
    * This value is specified in the Spark Structured Streaming + Kafka Integration Guide.
    */
  val keyColumn = "key"

  /**
    * Data set column used for values when reading/writing data from/to Kafka topics.
    * This value is specified in the Spark Structured Streaming + Kafka Integration Guide.
    */
  val valueColumn = "value"

}
