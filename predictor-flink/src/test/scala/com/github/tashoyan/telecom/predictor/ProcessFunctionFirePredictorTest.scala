package com.github.tashoyan.telecom.predictor

class ProcessFunctionFirePredictorTest extends AbstractFirePredictorTest {

  override def firePredictor(problemTimeoutMillis: Long, eventOutOfOrdernessMillis: Long): FlinkFirePredictor =
    new ProcessFunctionFirePredictor(problemTimeoutMillis, eventOutOfOrdernessMillis)

}
