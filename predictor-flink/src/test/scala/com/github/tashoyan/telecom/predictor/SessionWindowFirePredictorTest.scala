package com.github.tashoyan.telecom.predictor

import _root_.org.junit.Before

class SessionWindowFirePredictorTest extends AbstractFirePredictorTest {

  @Before def initFirePredictor(): Unit = {
    firePredictor = new SessionWindowFirePredictor(problemTimeoutMillis, eventOutOfOrdernessMillis)
  }

}
