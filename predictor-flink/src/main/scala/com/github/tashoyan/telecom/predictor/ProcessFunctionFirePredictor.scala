package com.github.tashoyan.telecom.predictor

import com.github.tashoyan.telecom.event.FireAlarmUtil._
import com.github.tashoyan.telecom.event.{Alarm, Event, FireAlarmState}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.util.Collector

class ProcessFunctionFirePredictor(
    override val problemTimeoutMillis: Long,
    eventOutOfOrdernessMillis: Long
) extends AbstractFlinkFirePredictor(eventOutOfOrdernessMillis) {

  private object AlarmDetector extends KeyedProcessFunction[Long, Event, Alarm] {
    private implicit val problemTimeoutMillis0: Long = problemTimeoutMillis
    private val eventOutOfOrdernessMillis0 = eventOutOfOrdernessMillis

    private lazy val state: ValueState[FireAlarmState] = {
      val stateName = classOf[FireAlarmState].getSimpleName
      val stateDescriptor = new ValueStateDescriptor[FireAlarmState](stateName, classOf[FireAlarmState])
      getRuntimeContext
        .getState(stateDescriptor)
    }

    override def processElement(event: Event, ctx: KeyedProcessFunction[Long, Event, Alarm]#Context, out: Collector[Alarm]): Unit = {
      val previousFireAlarmState = Option(state.value())
        .getOrElse(new FireAlarmState())
      val fireAlarmState = previousFireAlarmState.update(Iterator(event))
      state.update(fireAlarmState)
      ctx.timerService().registerEventTimeTimer(event.timestamp + eventOutOfOrdernessMillis0)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, Event, Alarm]#OnTimerContext, out: Collector[Alarm]): Unit = {
      val fireAlarm = Option(state.value())
        .flatMap(_.getFireAlarm)
      fireAlarm.foreach { alarm =>
        out.collect(alarm)
        state.clear()
      }
    }

  }

  override def predictAlarms(events: DataStream[Event]): DataStream[Alarm] = {
    val fireCandidates = events
      .filter(e => isFireCandidate(e))
      .assignTimestampsAndWatermarks(TimestampAssigner)
      .keyBy(_.siteId)

    val alarms: DataStream[Alarm] = fireCandidates.process(AlarmDetector)
    alarms
  }

}
