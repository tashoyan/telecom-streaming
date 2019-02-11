package com.github.tashoyan.telecom.predictor

import java.sql.Timestamp

//TODO Make generic and move to shared-resources: heatTimestamp -> triggerTimestamp
case class ProblemState(siteId: Long, heatTimestamp: Timestamp)
