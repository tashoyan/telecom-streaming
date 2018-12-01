package com.github.tashoyan.telecom.generator

import com.github.tashoyan.telecom.util.DataFrames.RichDataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Dataset, Row, SparkSession}

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

    val schema = spark.read
      .parquet(config.schemaFile)
      .schema

    val inputEvents: Dataset[Row] = spark.readStream
      .schema(schema)
      .parquet(config.inputDir)

    val kafkaEvents = inputEvents
      .withJsonColumn("value")
      .withColumn(
        "key",
        hash(col("operationcontextname"), col("identifier")) cast StringType
      )
      .repartition(col("operationcontextname"), col("identifier"))

    val query = kafkaEvents
      .writeStream
      .outputMode(OutputMode.Append())
      /* partitionBy is ignored by Kafka sink, maybe makes sense for Parquet*/
      //      .partitionBy("operationcontextname", "identifier")
      .queryName(getClass.getSimpleName)
      .format("kafka")
      .option("kafka.bootstrap.servers", config.kafkaBrokers)
      .option("topic", config.kafkaTopic)
      .option("checkpointLocation", config.checkpointDir)
      .start()
    query.awaitTermination()
  }

}
