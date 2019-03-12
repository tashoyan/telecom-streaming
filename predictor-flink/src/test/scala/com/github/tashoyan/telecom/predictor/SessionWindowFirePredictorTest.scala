package com.github.tashoyan.telecom.predictor

class SessionWindowFirePredictorTest extends AbstractFirePredictorTest {

  override def firePredictor(problemTimeoutMillis: Long, eventOutOfOrdernessMillis: Long): FlinkFirePredictor =
    new SessionWindowFirePredictor(problemTimeoutMillis, eventOutOfOrdernessMillis)

}
