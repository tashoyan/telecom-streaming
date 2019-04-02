package com.github.tashoyan.telecom.predictor

import java.util.concurrent.TimeUnit
import java.util.{Optional, Properties}

import com.github.tashoyan.telecom.event.Alarm
import com.github.tashoyan.telecom.flink.{AlarmSerializationSchema, KafkaFlinkEventReceiver}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner
import org.apache.kafka.clients.producer.ProducerConfig

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
TODO If the source does not send the data, then operations based on event time cannot progress.
 https://ci.apache.org/projects/flink/flink-docs-release-1.7/dev/event_time.html#idling-sources
 Kafka notes:
 https://ci.apache.org/projects/flink/flink-docs-stable/dev/connectors/kafka.html#kafka-consumers-and-timestamp-extractionwatermark-emission
 https://issues.apache.org/jira/browse/FLINK-5479
 Workaround: send heartbeat messages (poor workaround, garbage on the wire)
*/
/*
TODO Offsets of consumed metrics can be visible only by means of metrics:
 https://ci.apache.org/projects/flink/flink-docs-release-1.7/dev/connectors/kafka.html#kafka-connector-metrics
 Note: Flink Kafka Connector does not use consumer groups, it uses assign() Kafka API call;
 so no way to monitor Flink Kafka Connector progress via kafka-consumer-groups.sh.
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

    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    /*Checkpointing: https://ci.apache.org/projects/flink/flink-docs-release-1.7/dev/stream/state/checkpointing.html*/
    val checkpointIntervalSeconds = 5L
    env.enableCheckpointing(TimeUnit.SECONDS.toMillis(checkpointIntervalSeconds))
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(TimeUnit.SECONDS.toMillis(checkpointIntervalSeconds / 2))

    /*Restart strategies: https://ci.apache.org/projects/flink/flink-docs-release-1.7/dev/restart_strategies.html*/
    val restartAttempts = 3
    val restartTimeoutSeconds = 5L
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(restartAttempts, Time.seconds(restartTimeoutSeconds)))

    /*State backend: https://ci.apache.org/projects/flink/flink-docs-release-1.7/ops/state/state_backends.html*/
    val stateBackend: StateBackend = new FsStateBackend(config.checkpointDir)
    env.setStateBackend(stateBackend)

    val eventReceiver = new KafkaFlinkEventReceiver(config.kafkaBrokers, config.kafkaEventTopic)
    val events = eventReceiver.receiveEvents()

    val firePredictor = new CepFirePredictor(config.problemTimeoutMillis, config.watermarkIntervalMillis)
    val alarms = firePredictor.predictAlarms(events)

    val producerProps = new Properties
    producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafkaBrokers)
    val kafkaProducer = new FlinkKafkaProducer[Alarm](
      config.kafkaAlarmTopic,
      new AlarmSerializationSchema,
      producerProps,
      /* Partition according to keys defined by the serialization schema */
      Optional.empty[FlinkKafkaPartitioner[Alarm]]()
    )
    kafkaProducer.setWriteTimestampToKafka(true)
    alarms.addSink(kafkaProducer)

    env.execute(this.getClass.getSimpleName)
    ()
  }

}
