package com.github.tashoyan.telecom.spark

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, from_json, struct, to_json}
import org.apache.spark.sql.types.StructType

object DataFrames {

  implicit class RichDataFrame(val df: DataFrame) extends AnyVal {

    @inline def withJsonColumn(jsonColumn: String): DataFrame = {
      val structColumn = "struct"
      val columns = df.columns
        .map(col)
      val outputColumns = columns :+ col(jsonColumn)
      df.withColumn(structColumn, struct(columns: _*))
        .withColumn(jsonColumn, to_json(col(structColumn)))
        .select(outputColumns: _*)
    }

    @inline def parseJsonColumn(jsonColumn: String, schema: StructType): DataFrame = {
      val parsedJsonColumn = "parsed_json"
      val parsedColumns = schema.fields
        .map { field =>
          val column = field.name
          col(s"$parsedJsonColumn.$column") as column
        }
      val outputColumns = df.columns.map(col) ++ parsedColumns
      df.withColumn(parsedJsonColumn, from_json(col(jsonColumn), schema))
        .select(outputColumns: _*)
    }

  }

  implicit class RichDataset[T](val ds: Dataset[T]) extends AnyVal {

    @inline def withJsonColumn(jsonColumn: String): DataFrame = {
      ds.toDF()
        .withJsonColumn(jsonColumn)
    }

  }

}
