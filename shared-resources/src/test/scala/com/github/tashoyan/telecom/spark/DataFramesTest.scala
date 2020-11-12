package com.github.tashoyan.telecom.spark

import java.sql.Timestamp

import com.github.tashoyan.telecom.spark.DataFrames.RichDataFrame
import com.github.tashoyan.telecom.test.{JsonTestHarness, SparkTestHarness}
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class DataFramesTest extends AnyFunSuite with SparkTestHarness with JsonTestHarness with Matchers {

  test("withJsonColumn") {
    val spark0 = spark
    import spark0.implicits._

    val origColumns = Seq("int_column", "long_column", "double_column", "string_column", "timestamp_column")
    val sample = Seq(
      (1, 10L, 2.5, "one", new Timestamp(1001L))
    )
      .toDF("int_column", "long_column", "double_column", "string_column", "timestamp_column")
    val resultDf = sample.withJsonColumn("json_column")

    assert(resultDf.columns.length === origColumns.length + 1, "Expected to have all original columns plus JSON column")
    origColumns.foreach { column =>
      assert(resultDf.columns.contains(column))
    }

    val jsonStr = resultDf.select("json_column")
      .as[String]
      .head()

    val parsedResult = jsonToMap(jsonStr)
    parsedResult("int_column") shouldBe 1
    parsedResult("long_column") shouldBe 10L
    parsedResult("double_column") shouldBe 2.5
    parsedResult("string_column") shouldBe "one"
    parsedResult("timestamp_column") should matchEpochMillis(1001L)
  }

  test("parseJsonColumn") {
    val spark0 = spark
    import spark0.implicits._

    val schema = StructType(Array(
      StructField("int_column", IntegerType),
      StructField("long_column", LongType),
      StructField("double_column", DoubleType),
      StructField("string_column", StringType),
      StructField("timestamp_column", TimestampType)
    ))
    val sample = Map[String, Any](
      "int_column" -> 1,
      "long_column" -> 10L,
      "double_column" -> 2.5,
      "string_column" -> "one",
      "timestamp_column" -> "1970-01-01T00:00:01.001Z"
    )
    val sampleJson = mapToJson(sample)

    val sampleDf = Seq(
      ("something", sampleJson)
    ).toDF("some_column", "json_column")

    val resultDf = sampleDf.parseJsonColumn("json_column", schema)

    assert(resultDf.columns.length === 7, "Expected to retain all existing columns and add new parsed columns")
    assert(resultDf.count() === 1, "Same count as on input")

    val result = resultDf.head()
    result.getAs[String]("some_column") shouldBe "something"
    result.getAs[String]("json_column") shouldBe sampleJson
    result.getAs[Int]("int_column") shouldBe 1
    result.getAs[Long]("long_column") shouldBe 10L
    result.getAs[Double]("double_column") shouldBe 2.5
    result.getAs[String]("string_column") shouldBe "one"
    result.getAs[Timestamp]("timestamp_column") shouldBe new Timestamp(1001L)
  }

}
