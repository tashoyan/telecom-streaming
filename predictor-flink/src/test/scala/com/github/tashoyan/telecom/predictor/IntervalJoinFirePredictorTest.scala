package com.github.tashoyan.telecom.predictor

import _root_.org.junit.Before

class IntervalJoinFirePredictorTest extends AbstractFirePredictorTest {

  @Before def initFirePredictor(): Unit = {
    firePredictor = new IntervalJoinFirePredictor(problemTimeoutMillis, eventOutOfOrdernessMillis)
  }

}
