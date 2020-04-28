# spark-docker
Using scala/spark to analyze an httpd log dataset and deploying as a dockerfile.

## Problem Statement
Write a program in Scala that downloads the dataset at ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz 
and uses Apache Spark to determine the top-n most frequent visitors and urls for each day of the trace.

