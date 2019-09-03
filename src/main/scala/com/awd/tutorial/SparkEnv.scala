package com.awd.tutorial

import org.apache.spark.sql.SparkSession

trait SparkEnv {

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local[*]")
      .appName("Tutorial")
      .config("spark.eventLog.enabled", "true")
      .getOrCreate()
  }

}