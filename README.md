# de-tutorial
Data Engineering Tutorial

# Branch: pyspark-master
1. pyspark-master - python notebook with task to be completed in pyspark

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

# Task Example Answers
1. Add input path - readFile In[5]
2. Function to dynamically read multiple inputs and union into one DataFrame - readMultipleCSV In[6]
3. Function to dynamically unpivot US population statistics - unpivot In[9]
4. Broadcast Join state mapping onto US population statistics - In[13]
5. Join output of task 3 onto Nonemployer statistcs - In[14]
6. Write output to parquet partitioned by state name - In[15]
