package com.github.tashoyan.telecom.test

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, Suite}

trait SparkTestHarness extends Suite with BeforeAndAfter {

  protected implicit var spark: SparkSession = _

  before {
    spark = SparkSession
      .builder()
      .appName(getClass.getSimpleName)
      .config("spark.master", "local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.unsafe", "true")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.warehouse.dir", s"${sys.props("java.io.tmpdir")}/spark-warehouse")
      .getOrCreate()
  }

  after {
    spark.stop()
  }

}
