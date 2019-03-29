package com.github.tashoyan.telecom.predictor

class CepFirePredictorTest extends AbstractFirePredictorTest {

  override def firePredictor(problemTimeoutMillis: Long, eventOutOfOrdernessMillis: Long): FlinkFirePredictor =
    new CepFirePredictor(problemTimeoutMillis, eventOutOfOrdernessMillis)

}
