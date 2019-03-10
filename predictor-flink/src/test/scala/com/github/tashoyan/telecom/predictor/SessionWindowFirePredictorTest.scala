package com.github.tashoyan.telecom.predictor

import _root_.org.junit.Before
import org.apache.flink.test.util.AbstractTestBase

class SessionWindowFirePredictorTest extends AbstractTestBase with AbstractFirePredictorTest {

  @Before def initFirePredictor(): Unit = {
    firePredictor = new SessionWindowFirePredictor(problemTimeoutMillis, eventOutOfOrdernessMillis)
  }

}
