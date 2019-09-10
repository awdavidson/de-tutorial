# de-tutorial
Data Engineering Tutorial

# Branch: scala-task
1. scala-task - scala project with task to be completed in scala

# Data
1. US Nonemployer Statistcs 2014 - 2017
2. US population as state and county level 2010 - 2018
3. US numeric state ID to state name mapping

# Process
1. Load multiple Nonemployer Statistics and extract year from input path
2. Load and unpivot population statistics
3. Load and join state mapping to population statistics
4. Join output of step 3 into Nonemployer Statistics
5. Write output to parquet partitioned by state

# Task - Please see questions.txt
1. Add input path - com.awd.tutorial.utilities.inputReader
2. Function to dynamically read multiple inputs and union into one DataFrame - com.awd.tutorial.utilities.inputReader
3. Function to dynamically unpivot US population statistics - com.awd.tutorial.utilities.transform
4. Broadcast Join state mapping onto US population statistics - com.awd.tutorial.Main
5. Join output of task 3 onto Nonemployer statistcs - com.awd.tutorial.Main
6. Write output to parquet partitioned by state name - come.awd.tutorial.Main
