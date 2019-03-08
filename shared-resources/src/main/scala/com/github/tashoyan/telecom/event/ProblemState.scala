package com.github.tashoyan.telecom.event

//TODO Spark-specific, should use SparkEvent instead of Event
case class ProblemState(triggerEvent: Event)
