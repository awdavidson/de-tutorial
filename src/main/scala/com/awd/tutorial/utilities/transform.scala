package com.awd.tutorial.utilities

import com.awd.tutorial.SparkEnv

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object transform extends SparkEnv {

  import spark.implicits._

  def unpivot(df: DataFrame, by: Seq[String], key: String, value: String): DataFrame = {
    val (cols, types) = df.dtypes.filter{case (c, _) => !by.contains(c)}.unzip

    require(types.distinct.size == 1, s"${types.distinct.toString}.length != 1")

    val kvs = explode(array(
      cols.map(c => struct(lit(c).alias(key), col(c).alias(value))):_*
    ))

    val byExprs = by.map(col(_))

    df.select(byExprs :+ kvs.alias("_kvs"): _*)
      .select(byExprs ++ Seq($"_kvs.${key}", $"_kvs.${value}"): _*)
      .filter($"${value}".isNotNull)
  }

  def skewJoin(left: DataFrame, right: DataFrame, keys: Seq[String], chunks: Int): DataFrame = {
    val partKey = "_extra_key_"

    val leftWithKey = left.withColumn(partKey, (rand() * chunks).cast("Integer"))
    val rightWithKey = right.join(leftWithKey.select(partKey, keys:_*).distinct, keys, "inner")

    //Return the joined DataFrame
    leftWithKey.join(rightWithKey, partKey +: keys, "left_outer").drop("_extra_key_")
  }

}
