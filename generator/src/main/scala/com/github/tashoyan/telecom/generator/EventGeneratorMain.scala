package com.github.tashoyan.telecom.generator

import java.sql.Timestamp
import java.util.concurrent.TimeUnit

import com.github.tashoyan.telecom.event.Event._
import com.github.tashoyan.telecom.spark.DataFrames.RichDataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StringType

object EventGeneratorMain extends EventGeneratorArgParser {
  private val spark = SparkSession.builder()
    .getOrCreate()
  spark.sparkContext
    .setLogLevel("WARN")

  def main(args: Array[String]): Unit = {
    parser.parse(args, EventGeneratorConfig()) match {
      case Some(config) => doMain(config)
      case None => sys.exit(1)
    }
  }

  //TODO Refactor and enable scalastyle
  //scalastyle:off
  private def doMain(config: EventGeneratorConfig): Unit = {
    println(config)
    /*
    TODO: Explain in the article: In production, schema inference is not recommended:
    http://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#schema-inference-and-partition-of-streaming-dataframesdatasets
    */
    val schema = spark.read
      .parquet(config.schemaFile)
      .schema

    val inputEvents = spark.readStream
      .schema(schema)
      .parquet(config.inputDir)

    //TODO Configurable event timestamp interval
    val timestampIntervalSec = TimeUnit.MINUTES.toSeconds(1)
    val eventTimestampUdf = udf { (currentTimeSec: Long, random: Double) =>
      val eventTimestampSec = currentTimeSec - math.round(random * timestampIntervalSec)
      new Timestamp(TimeUnit.SECONDS.toMillis(eventTimestampSec))
    }
    val events = inputEvents
      .withColumn("current_time_seq", unix_timestamp())
      .withColumn("random", rand())
      .withColumn(timestampColumn, eventTimestampUdf(col("current_time_seq"), col("random")))
      .drop("current_time_seq", "random")

    val kafkaEvents = events
      .withJsonColumn(valueColumn)
      /*
      TODO Explain in the article:
      Kafka producer partition assignment: records having the same key go to the same topic partition.
      We ensure that events from the same site go through the same partition and preserve their order.
      Note that each Spark executor may send records to each Kafka broker hosting a partition.
      */
      //TODO Exactly once delivery: Kafka transactions and https://issues.apache.org/jira/browse/SPARK-25005
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
  //scalastyle:on

}
