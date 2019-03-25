package com.github.tashoyan.telecom.predictor

/*
TODO Checkpoints
 https://ci.apache.org/projects/flink/flink-docs-release-1.7/dev/stream/state/checkpointing.html
 https://ci.apache.org/projects/flink/flink-docs-release-1.7/dev/connectors/kafka.html#kafka-consumers-and-fault-tolerance
*/
/*
TODO Partition discovery when adding new Kafka partitions:
 https://ci.apache.org/projects/flink/flink-docs-release-1.7/dev/connectors/kafka.html#kafka-consumers-topic-and-partition-discovery
*/
/*
TODO Idle Kafka partition may cause no progress for watermarks:
 https://issues.apache.org/jira/browse/FLINK-5479
*/
//TODO Parallelism: Consume in parallel from all Kafka partitions: parallelism = k * n_kafka_partitions
object FlinkPredictorMain extends FlinkPredictorArgParser {

  def main(args: Array[String]): Unit = {
    parser.parse(args, FlinkPredictorConfig()) match {
      case Some(config) => doMain(config)
      case None => sys.exit(1)
    }
  }

  private def doMain(config: FlinkPredictorConfig): Unit = {
    println(config)
  }

}
