package com.awd.tutorial

import com.awd.tutorial.utilities.{inputReader, transform}
import org.apache.spark.sql.functions._

object Main extends SparkEnv {

  def main(args: Array[String]): Unit = {

    import spark.implicits._

    // Add new properties for population table as it is '|' delimited opposed to ',' delimited
    val popProperties = Map(
      "header" -> "true",
      "delimiter" -> "|",
      "inferSchema" -> "false",
      "ignoreLeadingWhiteSpace" -> "true",
      "ignoreTrailingWhiteSpace" -> "true",
      "quote" -> "\"",
      "escape" -> "\""
    )



    // Read nonEmp data: raw_path = true as we want to pull the date from file name; rename ST and CTY
    val nonEmp = inputReader.readMultipleCSV(s"${args(0)}", "nonemp", true)
      .withColumn("YEAR", regexp_extract($"source_path", "([0-9]{4})", 1))
      .withColumnRenamed("ST", "STATE")
      .withColumnRenamed("CTY", "COUNTY")

    // Read states this is a mapping from ID to State Name
    val states = inputReader.readFile(s"${args(0)}/states.csv")

    /**
      * Read state population data and unpivot data e.g.
      * Current:
      * STATE|POP_2018|POP_2017
      *     x|      20|      22
      *
      * We want:
      * STATE|YEAR|POP
      *     x|2018| 20
      *     x|2017| 22
      */
    val pop = inputReader.readFile(s"${args(0)}/sub-est2018_all.csv", popProperties)
    val unpivotPop = transform.unpivot(pop.drop("SUMLEV", "PLACE", "COUSUB", "CONCIT", "PRIMGEO_FLAG", "FUNCSTAT", "CENSUS2010POP", "ESTIMATESBASE2010"), Seq("STATE", "COUNTY", "NAME"), "YEAR", "POPULATION").persist()

    // Population currently broken down to towns, want to group by STATE, COUNTY and YEAR and sum population
    val groupPop = unpivotPop.groupBy($"STATE", $"COUNTY", $"YEAR").agg(sum("POPULATION").as("POPULATION"))

    // Enrich population table with state names, extract year from unpivoted key and rename value to population
    val enrichPop = groupPop.join(broadcast(states), Seq("STATE"), "left_outer")
      .withColumn("YEAR", regexp_extract($"YEAR", "([0-9]{4})", 1))

    // Enrich nonEmp data with state populations
    val outputDF = nonEmp.join(enrichPop, Seq("STATE", "COUNTY", "YEAR"), "left_outer")

    // Store to parquet partitioned by state
    outputDF.write
      .mode("Overwrite")
      .partitionBy("STNAME")
      .parquet("output/data.parq")
    
  }

}
