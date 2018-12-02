package com.github.tashoyan.telecom.sampler

import java.io.{File, FileFilter}
import java.nio.file.Files
import java.sql.Timestamp

import com.github.tashoyan.telecom.event.Event
import com.github.tashoyan.telecom.event.Event._
import org.apache.commons.io.FileUtils.deleteDirectory
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}

class Sampler(val eventsPerStation: Int)(implicit spark: SparkSession) {

  def generateEvents(stations: Dataset[Integer]): Dataset[Event] = {
    import spark.implicits._

    val stationCount = eventsPerStation
    val stationsUdf = udf { station: Integer =>
      Seq.fill(stationCount)(station.toLong)
    }
    val infoUdf = udf { siteId: Long =>
      s"Communication failure at site $siteId"
    }

    stations
      .withColumn("stations", stationsUdf(col("station")))
      .withColumn(siteIdColumn, explode(col("stations")))
      .withColumn(idColumn, monotonically_increasing_id())
      //Let the Event Generator set timestamps of the generated events
      .withColumn(timestampColumn, lit(new Timestamp(0L)))
      .withColumn(severityColumn, lit("MAJOR"))
      .withColumn(infoColumn, infoUdf(col(siteIdColumn)))
      .drop("station", "stations")
      .as[Event]
  }

}

object Sampler {

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
