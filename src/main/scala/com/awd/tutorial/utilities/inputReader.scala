package com.awd.tutorial.utilities

import com.awd.tutorial.SparkEnv

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object inputReader extends SparkEnv {

  import spark.implicits._

  val properties = Map(
    "header" -> "true",
    "delimiter" -> ",",
    "inferSchema" -> "false",
    "ignoreLeadingWhiteSpace" -> "true",
    "ignoreTrailingWhiteSpace" -> "true",
    "quote" -> "\"",
    "escape" -> "\""
  )

  def readFile(dataFilePath: String, fileProperties: Map[String, String] = properties, source_path: Boolean = false): DataFrame = {

    val inputDF = spark.read
      .format("csv")
      .option("quote", "")
      .option("header", fileProperties.get("header").get)
      .option("delimiter", fileProperties.get("delimiter").get)
      .option("inferSchema", fileProperties.get("inferSchema").get)
      .option("ignoreLeadingWhiteSpace", fileProperties.get("ignoreLeadingWhiteSpace").get)
      .option("ignoreTrailingWhiteSpace", fileProperties.get("ignoreTrailingWhiteSpace").get)
      .option("quote", fileProperties.get("quote").get)
      .option("escape", fileProperties.get("escape").get)
      .load(dataFilePath)


    if (source_path) inputDF.withColumn("source_path", input_file_name()) else inputDF

  }

  def readMultipleCSV(source_path: String, table: String, raw_path: Boolean = false): DataFrame = {

    var inputs = List[String]()
    var inputDF = spark.emptyDataFrame
    var intersect_columns = Array[String]()

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fs.listStatus(new Path(source_path))
      .filter(_.toString.contains(s"${table}"))
      .map { file: FileStatus =>
        inputs = file.getPath.toString :: inputs
      }

    val sourceDF = inputs.collect {
      case path: String =>
        inputDF = readFile(path, properties, raw_path)
        if (intersect_columns.length > 1) intersect_columns = intersect_columns.intersect(inputDF.columns) else intersect_columns = inputDF.columns
        inputDF
    }
    sourceDF.reduce((x, y) => x.select(intersect_columns.head, intersect_columns.tail: _*) union (y.select(intersect_columns.head, intersect_columns.tail: _*)))
  }

}
