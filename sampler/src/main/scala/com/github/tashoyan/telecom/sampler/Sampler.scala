package com.github.tashoyan.telecom.sampler

import java.io.{File, FileFilter}
import java.nio.file.Files
import java.sql.Timestamp

import com.github.tashoyan.telecom.event.Event
import org.apache.commons.io.FileUtils.deleteDirectory
import org.apache.spark.sql.Dataset

object Sampler {

  /**
    * Generates event sample evenly distributed per stations and within a time range.
    *
    * @param stations             Stations - sources of the events.
    * @param timeRangeMillis      - Time range width in ms, where the events will have their timestamps.
    * @param perStationMultiplier - How many events generate for each station.
    *                             For each station, all events have the same timestamp.
    *                             The purpose is to simulate duplicated events.
    * @return Sample of events.
    *         The events will have a dummy timestamp,
    *         just the offset in milliseconds from the time range boundary.
    *         Event Generator is expected to substitute real timestamps:
    *         real_timestamp = current_time - offset
    */
  def generateEvents(stations: Seq[Int], timeRangeMillis: Long, perStationMultiplier: Int): Seq[Event] = {
    require(stations.nonEmpty, "stations set must be non empty")
    require(timeRangeMillis > 0, "timeRangeMillis must be > 0")
    require(perStationMultiplier > 0, "perStationMultiplier must be > 0")

    val eventIntervalMillis: Long = timeRangeMillis / stations.size

    stations
      .zipWithIndex
      .map { case (station, index) =>
        //Dummy timestamp - will be used as event offset within the event time range
        (station.toLong, new Timestamp(index * eventIntervalMillis))
      }
      .flatMap(Seq.fill(perStationMultiplier)(_))
      .map { case (siteId, timestamp: Timestamp) =>
        Event(timestamp, siteId, "MAJOR", s"Communication failure at site $siteId")
      }
  }

  def writeEvents(events: Dataset[Event], path: String): Unit = {
    events
      .repartition(1)
      .write
      .parquet(path)
    convertParquetDirToFile(path)
  }

  private def convertParquetDirToFile(path: String): Unit = {
    val dir = new File(path)
    if (!dir.isDirectory)
      throw new IllegalArgumentException(s"Not a directory: $dir")

    val filter = new FileFilter {
      override def accept(file: File): Boolean =
        file.isFile &&
          file.getName.endsWith(".parquet")
    }
    val parquetFiles = dir.listFiles(filter)
    if (parquetFiles.length != 1)
      throw new IllegalArgumentException(s"The directory does not contain exactly one Parquet file: $dir")

    val srcParquetFile = parquetFiles.head.toPath
    val dstParquetFile = new File(s"$path.parquet").toPath
    Files.move(srcParquetFile, dstParquetFile)
    deleteDirectory(dir)
  }

}
