package com.github.tashoyan.telecom.predictor

import com.github.tashoyan.telecom.event.{Alarm, Event}
import org.apache.flink.streaming.api.scala.{DataStream, _}

//TODO problemTimeoutMillis seems generic for this use case - move to the trait
class SessionWindowFirePredictor(val problemTimeoutMillis: Long) extends FirePredictor {

  override def predictAlarms(events: DataStream[Event]): DataStream[Alarm] = {
    //TODO Implement
    events.map(event => Alarm(event.timestamp, event.siteId, event.severity, event.info))
  }

}
