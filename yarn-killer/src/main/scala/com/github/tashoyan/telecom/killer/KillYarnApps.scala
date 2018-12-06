package com.github.tashoyan.telecom.killer

import scalaj.http.Http
import org.apache.spark.sql.SparkSession

import scala.util.control.NonFatal

class KillYarnApps {

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

    appIds.foreach { appId =>
      try {
        killApp(appId)
      } catch {
        case NonFatal(e) =>
          Console.err.println(s"Failed to kill app $appId, advancing to the next one. Exception: ${e.getMessage}")
      }
    }

  }

  private def killApp(appId: String): Unit = {
    val response = Http(s"http://$rmUrl/ws/v1/cluster/apps/$appId/state")
      .header("Content-Type", "application/json")
      .put("""{"state": "KILLED"}""")
      .asParamMap
    println(response)
  }

  //  private def httpPutJson(url: String, jsonData: String): (Int, String) = {
  //    val connection: HttpURLConnection = new URL(url).openConnection().asInstanceOf[HttpURLConnection]
  //    connection.setDoOutput(true)
  //    connection.setRequestMethod("PUT")
  //    connection.setRequestProperty("Content-Type", "application/json")
  //    var out: Writer = null
  //    try {
  //      out = new OutputStreamWriter(connection.getOutputStream)
  //      out.write(jsonData)
  //    } finally {
  //      if (out != null)
  //        out.close()
  //    }
  //
  //    val responseCode = connection.getResponseCode
  //    try {
  //      val responseText = connection.getInputStream.text
  //      new Tuple2<>(responseCode, responseText)
  //    } catch (IOException) {
  //      String responseText = connection.errorStream.text
  //      new Tuple2<>(responseCode, responseText)
  //    }
  //
  //  }

}
