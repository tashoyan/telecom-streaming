package com.github.tashoyan.telecom.predictor

import com.github.tashoyan.telecom.event.Event

//TODO Spark-specific, should use SparkEvent instead of Event
case class ProblemState(triggerEvent: Event)
