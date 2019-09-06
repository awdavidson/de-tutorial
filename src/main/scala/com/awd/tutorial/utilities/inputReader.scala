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

  // Task 1:

  def readFile(dataFilePath: String, fileProperties: Map[String, String] = properties): DataFrame = {

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

    inputDF

  }

  // Task 2:

}
