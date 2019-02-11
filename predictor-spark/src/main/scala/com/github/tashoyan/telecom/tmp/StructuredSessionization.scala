/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off
package com.github.tashoyan.telecom.tmp

import java.sql.Timestamp
import java.util.concurrent.TimeUnit

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming._

/**
  * Counts words in UTF8 encoded, '\n' delimited text received from the network.
  *
  * Usage: MapGroupsWithState <hostname> <port>
  * <hostname> and <port> describe the TCP server that Structured Streaming
  * would connect to receive data.
  *
  * To run this on your local machine, you need to first run a Netcat server
  * `$ nc -lk 9999`
  * and then run the example
  * `$ bin/run-example sql.streaming.StructuredSessionization
  * localhost 9999`
  */
object StructuredSessionization {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: StructuredSessionization <hostname> <port>")
      System.exit(1)
    }

    val host = args(0)
    val port = args(1).toLong

    val spark = SparkSession
      .builder
      .appName("StructuredSessionization")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    // Create DataFrame representing the stream of input lines from connection to host:port
    val lines = spark.readStream
      .format("socket")
      .option("host", host)
      .option("port", port)
      .option("includeTimestamp", true)
      .load()

    // Split the lines into words, treat words as sessionId of events
    val events = lines
      .as[(String, Timestamp)]
      .flatMap { case (line, timestamp) =>
        line.split(" ").map(word => Event(sessionId = word, timestamp))
      }

    val sessionFullFunc: (String, Iterator[Event], GroupState[SessionFull]) => SessionFull =
      { (sessionId, events, state) =>
        if (state.hasTimedOut) {
          println("++++++++++++++++++ TIMED OUT")
          val finalSession = state.get.copy(expired = true)
          state.remove()
          finalSession
        } else {
          println("++++++++++++++++++ NOT TIMED OUT")
          val msTimestamps = events.map(_.timestamp.getTime).toSeq
          println(s"++++++++++++++++++ TIMESTAMPS: ${msTimestamps.size}")
          if (state.exists) {
            println("++++++++++++++++++ EXISTS")
            val updatedNumEvents = state.get.numEvents + msTimestamps.size
            val updatedStartTimestampMs = state.get.startTimestampMs
            val updatedEndTimestampMs = math.max(state.get.endTimestampMs, msTimestamps.max)
            val updatedExpirationTimestamp = state.get.expirationTimestamp
            println(s"+++ updatedExpirationTimestamp: $updatedExpirationTimestamp, now: ${new Timestamp(System.currentTimeMillis())}")
            val updatedSession = SessionFull(
              id = sessionId,
              numEvents = updatedNumEvents,
              startTimestampMs = updatedStartTimestampMs,
              endTimestampMs = updatedEndTimestampMs,
              durationMs = updatedEndTimestampMs - updatedStartTimestampMs,
              expirationTimestamp = updatedExpirationTimestamp,
              expired = false
            )
            state.update(updatedSession)
            state.setTimeoutTimestamp(updatedExpirationTimestamp.getTime)
            updatedSession
          } else {
            println("++++++++++++++++++ NOT EXISTS")
            val expirationTimestamp = new Timestamp(msTimestamps.min + TimeUnit.SECONDS.toMillis(10))
            println(s"+++ expirationTimestamp: $expirationTimestamp, now: ${new Timestamp(System.currentTimeMillis())}")
            val newSession = SessionFull(
              id = sessionId,
              numEvents = msTimestamps.size,
              startTimestampMs = msTimestamps.min,
              endTimestampMs = msTimestamps.max,
              durationMs = msTimestamps.max - msTimestamps.min,
              expirationTimestamp,
              expired = false
            )
            state.update(newSession)
            state.setTimeoutTimestamp(expirationTimestamp.getTime)
            newSession
          }
        }
      }

    //    val sessionUpdateFunc: (String, Iterator[Event], GroupState[SessionInfo]) => SessionUpdate =
    //    { (sessionId, events, state) =>
    //      if (state.hasTimedOut) {
    //        println("++++++++++++++++++ TIMED OUT")
    //        state.remove()
    //        val finalUpdate = SessionUpdate(
    //          id = sessionId,
    //          durationMs = state.get.durationMs,
    //          numEvents = state.get.numEvents,
    //          expired = true
    //        )
    //        finalUpdate
    //      } else {
    //        println("++++++++++++++++++ NOT TIMED OUT")
    //        val msTimestamps = events.map(_.timestamp.getTime).toSeq
    //        println(s"++++++++++++++++++ TIMESTAMPS: ${msTimestamps.size}")
    //        if (state.exists) {
    //          println("++++++++++++++++++ EXISTS")
    //          import org.apache.spark.sql.execution.streaming.GroupStateImpl
    //          val timeoutTimestamp = state.asInstanceOf[GroupStateImpl[SessionInfo]].getTimeoutTimestamp
    //          println(s"+++ TIMEOUT TIMESTAMP: $timeoutTimestamp, now: ${System.currentTimeMillis()}")
    //          val updatedNumEvents = state.get.numEvents + msTimestamps.size
    //          val updatedStartTimestampMs = state.get.startTimestampMs
    //          val updatedEndTimestampMs = math.max(state.get.endTimestampMs, msTimestamps.max)
    //          val updatedSession = SessionInfo(
    //            numEvents = updatedNumEvents,
    //            startTimestampMs = updatedStartTimestampMs,
    //            endTimestampMs = updatedEndTimestampMs
    //          )
    //          val newTimeoutTimestamp = msTimestamps.min + TimeUnit.SECONDS.toMillis(10)
    //          println(s"+++ NEW TIMEOUT TIMESTAMP: $newTimeoutTimestamp")
    //          state.update(updatedSession)
    //          state.setTimeoutTimestamp(newTimeoutTimestamp)
    //          val sessionUpdate = SessionUpdate(
    //            id = sessionId,
    //            durationMs = updatedEndTimestampMs - updatedStartTimestampMs,
    //            numEvents = updatedNumEvents,
    //            expired = false
    //          )
    //          sessionUpdate
    //        } else {
    //          println("++++++++++++++++++ NOT EXISTS")
    //          val newSession = SessionInfo(
    //            numEvents = msTimestamps.size,
    //            startTimestampMs = msTimestamps.min,
    //            endTimestampMs = msTimestamps.max
    //          )
    //          val timeoutTimestamp = msTimestamps.min + TimeUnit.SECONDS.toMillis(10)
    //          println(s"+++ TIMEOUT TIMESTAMP: $timeoutTimestamp, now: ${System.currentTimeMillis()}")
    //          state.update(newSession)
    //          state.setTimeoutTimestamp(timeoutTimestamp)
    //          val sessionUpdate = SessionUpdate(
    //            id = sessionId,
    //            durationMs = msTimestamps.max - msTimestamps.min,
    //            numEvents = msTimestamps.size,
    //            expired = false
    //          )
    //          sessionUpdate
    //        }
    //      }
    //
    //    }

    val eventTimestampSessions = events
      .withWatermark("timestamp", "10 seconds")
      .groupByKey(_.sessionId)
      .mapGroupsWithState(GroupStateTimeout.EventTimeTimeout())(sessionFullFunc)

    // Sessionize the events. Track number of events, start and end timestamps of session, and
    // and report session updates.
    //    val sessionUpdates = events
    //      .groupByKey(event => event.sessionId)
    //      .mapGroupsWithState[SessionInfo, SessionUpdate](GroupStateTimeout.ProcessingTimeTimeout) {
    //
    //        case (sessionId: String, events: Iterator[Event], state: GroupState[SessionInfo]) =>
    //
    //          // If timed out, then remove session and send final update
    //          if (state.hasTimedOut) {
    //            val finalUpdate =
    //              SessionUpdate(sessionId, state.get.durationMs, state.get.numEvents, expired = true)
    //            state.remove()
    //            finalUpdate
    //          } else {
    //            // Update start and end timestamps in session
    //            val timestamps = events.map(_.timestamp.getTime).toSeq
    //            val updatedSession = if (state.exists) {
    //              val oldSession = state.get
    //              SessionInfo(
    //                oldSession.numEvents + timestamps.size,
    //                oldSession.startTimestampMs,
    //                math.max(oldSession.endTimestampMs, timestamps.max))
    //            } else {
    //              SessionInfo(timestamps.size, timestamps.min, timestamps.max)
    //            }
    //            state.update(updatedSession)
    //
    //            // Set timeout such that the session will be expired if no data received for 10 seconds
    //            state.setTimeoutDuration("10 seconds")
    //            SessionUpdate(sessionId, state.get.durationMs, state.get.numEvents, expired = false)
    //          }
    //      }

    // Start running the query that prints the session updates to the console
    val query = eventTimestampSessions
      .writeStream
      .outputMode("update")
      .option("truncate", false)
      .format("console")
      .start()

    query.awaitTermination()
  }
}
/** User-defined data type representing the input events */
case class Event(sessionId: String, timestamp: Timestamp)

/**
  * User-defined data type for storing a session information as state in mapGroupsWithState.
  *
  * @param numEvents        total number of events received in the session
  * @param startTimestampMs timestamp of first event received in the session when it started
  * @param endTimestampMs   timestamp of last event received in the session before it expired
  */
case class SessionInfo(
    numEvents: Int,
    startTimestampMs: Long,
    endTimestampMs: Long
) {

  /** Duration of the session, between the first and last events */
  def durationMs: Long = endTimestampMs - startTimestampMs
}

/**
  * User-defined data type representing the update information returned by mapGroupsWithState.
  *
  * @param id          Id of the session
  * @param durationMs  Duration the session was active, that is, from first event to its expiry
  * @param numEvents   Number of events received by the session while it was active
  * @param expired     Is the session active or expired
  */
case class SessionUpdate(
    id: String,
    durationMs: Long,
    numEvents: Int,
    expired: Boolean
)

case class SessionFull(
    id: String,
    numEvents: Int,
    startTimestampMs: Long,
    endTimestampMs: Long,
    durationMs: Long,
    expirationTimestamp: Timestamp,
    expired: Boolean
)

