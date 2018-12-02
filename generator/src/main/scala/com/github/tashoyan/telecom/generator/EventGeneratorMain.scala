package com.github.tashoyan.telecom.generator

import com.github.tashoyan.telecom.event.Event._
import com.github.tashoyan.telecom.spark.DataFrames.RichDataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StringType

object EventGeneratorMain extends EventGeneratorArgParser {

  def main(args: Array[String]): Unit = {
    parser.parse(args, EventGeneratorConfig()) match {
      case Some(config) => doMain(config)
      case None => sys.exit(1)
    }
  }

  private def doMain(config: EventGeneratorConfig): Unit = {
    val spark = SparkSession.builder()
      .appName(getClass.getSimpleName)
      .getOrCreate()
    spark.sparkContext
      .setLogLevel("WARN")

    //    val schema = spark.read
    //      .parquet(config.schemaFile)
    //      .schema

    val inputEvents = spark.readStream
        .option("inferSchema", value = true)
      //      .schema(schema)
      .parquet(config.inputDir)

    val kafkaEvents = inputEvents
      .withJsonColumn(valueColumn)
      /*
      TODO Explain in the article:
      Kafka producer partition assignment: records having the same key go to the same topic partition.
      We ensure that events from the same site go through the same partition and preserve their order.
      Note that each Spark executor may send records to each Kafka broker hosting a partition.
      */
      .withColumn(keyColumn, col(siteIdColumn) cast StringType)

    /*
    TODO Generator may produce duplicates.
    Kafka semantics: at least once. The Kafka producer may send a duplicate before getting ack from the broker.
    TODO - clarify in Kafka doc.
    Therefore, Correlator must handle duplicates (for ex., de-duplicate in each window by event id).
    */

    val query = kafkaEvents
      .writeStream
      .outputMode(OutputMode.Append())
      .queryName(getClass.getSimpleName)
      .format("kafka")
      .option("kafka.bootstrap.servers", config.kafkaBrokers)
      .option("topic", config.kafkaTopic)
      .option("checkpointLocation", config.checkpointDir)
      .start()
    query.awaitTermination()
  }

}
