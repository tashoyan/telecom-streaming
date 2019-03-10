package com.github.tashoyan.telecom.predictor

import com.github.tashoyan.telecom.event.Event
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time

abstract class AbstractFlinkFirePredictor(eventOutOfOrdernessMillis: Long) extends FlinkFirePredictor {

  protected object TimestampAssigner extends BoundedOutOfOrdernessTimestampExtractor[Event](Time.milliseconds(eventOutOfOrdernessMillis)) {
    override def extractTimestamp(event: Event): Long =
      event.timestamp
  }

}
