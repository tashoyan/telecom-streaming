package com.github.tashoyan.telecom.killer

import org.apache.spark.sql.SparkSession
import scalaj.http.Http

import scala.annotation.tailrec
import scala.util.control.NonFatal

object KillYarnApps {

  private val maxAttempts = 3

  private val spark = SparkSession.builder()
    .getOrCreate()
  spark.sparkContext
    .setLogLevel("WARN")

  private val rmUrl: String = Option(spark.sparkContext
    .hadoopConfiguration)
    .map(_.get("yarn.resourcemanager.webapp.address")).getOrElse {
    Console.err.println("Cannot get Resource Manager URL from the Hadoop configuration. Is HADOOP_CONF_DIR variable defined? Exiting.")
    sys.exit(1)
  }

  def main(args: Array[String]): Unit = {
    val appIds = args
    if (appIds.isEmpty) {
      Console.err.println("Expected app ids as a space separated list, but got nothing. Exiting.")
      sys.exit(1)
    }

    appIds.par
      .foreach { appId =>
        try {
          killApp(appId)
        } catch {
          case NonFatal(e) =>
            Console.err.println(s"Failed to kill app $appId. Exception: ${e.getMessage}")
        }
      }

  }

  private def killApp(appId: String): Unit =
    killApp(appId, 1)

  @tailrec
  private def killApp(appId: String, attempt: Int): Unit = {
    if (attempt <= maxAttempts) {
      val response = Http(s"http://$rmUrl/ws/v1/cluster/apps/$appId/state")
        .header("Content-Type", "application/json")
        .put("""{"state": "KILLED"}""")
        .asString
      if (response.code != 200)
        killApp(appId, attempt + 1)
      else
        Console.err.println(s"Killed app $appId on attempt $attempt")
    } else {
      Console.err.println(s"Failed to kill app $appId in $maxAttempts attempts")
    }
  }

}
