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

    // Please execute task 1 and 2 in utilities.inputReader

    // Step 1:

    // Step 2: Rename columns “ST” and “CTY” to “STATE” and “COUNTY” respectively. Create a “YEAR” column by parsing the year from the file name (hint: regexp_extract may be useful)

    // Step 3: Read US Population Statistics data (hint: check delimiter)

    // Task 3: Can either be created within Main or utilities.transform
    /**
      * Unpivot example:
      * Current:
      * STATE|POP_2018|POP_2017
      *     x|      20|      22
      *
      * We want:
      * STATE|YEAR|POP
      *     x|2018| 20
      *     x|2017| 22
      */

    // Step 4: Read US States Mapping Table
    val states = inputReader.readFile(s"${args(0)}/states.csv")

    // Task 4:

    // Task 5:

    // Task 6: Write Output to Parquet file partitioned by “STNAME” (path output/tutorial.parq)


  }

}
