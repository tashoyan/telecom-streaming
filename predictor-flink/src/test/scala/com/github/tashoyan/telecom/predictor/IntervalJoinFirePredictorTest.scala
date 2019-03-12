package com.github.tashoyan.telecom.predictor

class IntervalJoinFirePredictorTest extends AbstractFirePredictorTest {

  override def firePredictor(problemTimeoutMillis: Long, eventOutOfOrdernessMillis: Long): FlinkFirePredictor =
    new IntervalJoinFirePredictor(problemTimeoutMillis, eventOutOfOrdernessMillis)

}
