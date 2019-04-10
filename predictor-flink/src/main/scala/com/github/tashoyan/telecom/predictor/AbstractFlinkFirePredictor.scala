package com.github.tashoyan.telecom.predictor

import com.github.tashoyan.telecom.event.Event
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Common functions for Fire Predictor implementations based on Flink.
  *
  * @param eventOutOfOrdernessMillis Out-of-orderness interval in milliseconds.
  *                                  Events may come to the processor in a wrong order, but only within this time interval.
  */
abstract class AbstractFlinkFirePredictor(eventOutOfOrdernessMillis: Long) extends FlinkFirePredictor {

  protected object TimestampAssigner extends BoundedOutOfOrdernessTimestampExtractor[Event](Time.milliseconds(eventOutOfOrdernessMillis)) {
    override def extractTimestamp(event: Event): Long =
      event.timestamp
  }

}
