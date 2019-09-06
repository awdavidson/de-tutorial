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

    // Task 3: Create a function/implement logic to unpivot US Population Statistics data. Implementation can either be created within Main or utilities.transform
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

    // Task 4: Execute a broadcast join to join US States onto the unpivoted US Population Statistics data. What does this type of join do? When should it be used?

    // Task 5: Execute a standard join to join the output of task 4 onto the output of Step 1 (join key “STATE”, “COUNTY”, “YEAR”). Identify what type of join spark has executed through either the UI or physical plan (step.explain())

    // Task 6: Write Output to Parquet file partitioned by “STNAME” (path output/tutorial.parq)


  }

}
