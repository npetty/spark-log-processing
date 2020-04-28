package my.challenge

import my.challenge.LogEntry._
import java.io._
import java.util.zip._

import com.typesafe.config.{ConfigException, ConfigFactory}
import org.apache.spark.SparkFiles
import org.slf4j.{Logger, LoggerFactory}

import scala.io.Source
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
 * This is the entry point for the TopN application. It was designed to complete
 * with the following specs in mind.
 *
 * Write a program in Scala that downloads the dataset at
 * tp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz and
 * uses Apache Spark to determine the top-n most frequent
 * visitors and urls for each day of the trace.
 *
 * Many of the functions in this application were written to accept general
 * inputs, but to keep to the requirements, the app overall does not allow
 * for one to change inputs so that the original requirement is not met.
 */
object TopN extends App{

   /**
    *  Set default values for inputs
    */
    val MAIN_CONFIG_PATH = "my.challenge"
    val CONFIG_SPARK_MASTER = "spark.master"
    val DEFAULT_SPARK_MASTER = "local[*]"
    val CONFIG_SPARK_APP = "app-name"
    val DEFAULT_APP_NAME = "TopNUsers"
    val CONFIG_DATA_URL = "datasource.url"
    val DEFAULT_DATA_URL = "ftp://anonymous:blank@ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz"
    val CONFIG_N_VALUE = "app.n"
    val DEFAULT_N = 10
    val DEFAULT_RESOURCES_PATH = "src/main/resources/NASA_access_log_Jul95.gz"

    val VISITOR_COL = "visitor"
    val URL_COL = "url"
    val FINAL_VIEW = "finalView"

    //Initialize the logger
    def log : Logger = LoggerFactory.getLogger( getClass )

    //declare the spark session here outside of the try block so we can close it in a finally block.
    //this approach is recommended in the Scala cookbook.
    var session = None: Option[SparkSession]

    try {
     /**
      * Load config variables from application.conf file and set the spark config variables.
      */

     val extConfigFile = new File("conf/application.conf")
     val config =  if(extConfigFile.exists()) {
      val extConfig = ConfigFactory.parseFile(extConfigFile).getConfig(MAIN_CONFIG_PATH)
      log.info("loaded external config to override internal config")
      ConfigFactory.load(extConfig)
     }
     else {
      log.info("loaded internal config to override internal config")
      ConfigFactory.load().getConfig(MAIN_CONFIG_PATH)
     }
     val appName = if(config.hasPath(CONFIG_SPARK_APP)) config.getString(CONFIG_SPARK_APP) else DEFAULT_APP_NAME
     val sparkMaster = if(config.hasPath(CONFIG_SPARK_MASTER)) config.getString(CONFIG_SPARK_MASTER) else DEFAULT_SPARK_MASTER
     val my_n = if(config.hasPath(CONFIG_N_VALUE)) config.getInt(CONFIG_N_VALUE) else DEFAULT_N

     log.info("{} Initializing", appName)

     /**
      * Initialize the spark session using local mode
      */
     session = Some(SparkSession.builder
       .master(sparkMaster)
       .appName(appName)
       .getOrCreate())

     /**
      * Initialize the calculator class that will perform the work on the data set.
      */
     val calculator = new TopNCalculator(config)

     /**
      * Pull the datasource url from config
      */
     val datasource = if(config.hasPath(CONFIG_DATA_URL)) config.getString(CONFIG_DATA_URL) else DEFAULT_DATA_URL

     /**
      * Create an RDD of LogEntries with the follow steps
      *
      *  DataGrabber.getDataAsRdd will pull load from an external source or file
      *      and return an RDD.
      *  map(parseAccessLogEntry) will create a LogEntry object for each line of
      *      the access log file. parseAccessLogEntry is implemented in LogEntry
      *      object within this same project.
      *  filter step will remove any log entries with the host/client IP set to
      *      "Empty". This is a default used in the log parser when an invalid
      *      line is found.
      *  persist the newly created dataframe as it will be used in multiple
      *      processing steps.
      */
     val accessLogs = DataGrabber.getDataAsRdd(datasource, session.get.sparkContext)
       .map(parseAccessLogEntry)
       .filter(!_.visitor.equals(LogEntry.INVALID_CLIENT))
       .persist()

     log.info("Data successfully loaded and parsed into {} partitions", accessLogs.getNumPartitions)


     /**
      * Create a SqlContext in our session and create a dataframe. Since our
      * data is now fully structured, this will allow for better query experience.
      */
     val sqlContext = session.get.sqlContext
     val accessRecords = sqlContext.createDataFrame(accessLogs)
     //import sqlContext.implicits

     /**
      * For sanity check, print the first five lines
      */
     log.info("Printing a few log records to review")
     accessLogs.take(5).foreach( println )

     /**
      * Now we will run logic to produce the desired output of top-n users and urls
      * for each day. The TopNCalculator contains helper methods to implement the
      * following approach:
      *
      *  1.  Group the access logs on two columns, day and URL, calculating the count
      *      for each group.
      *  2.  Apply a Window Function, rank to the count column, partitioning by day.
      *      This will give an ordered rank by day for the number of hits on the URL.
      *  3.  Repeat steps 1 and 2, but for host/IP instead of URL. This will leave us
      *      with two data frames with columns "day", "url|host", "count", "daily_rank"
      *  4.  Join the two data frames on the day and rank columns. This will give us
      *      a single data frame with ranked hosts and urls for each day.
      *  5.  Query the single dataframe for records where daily_rank is less than or
      *      equal to N.
      */
     log.info("Calculating ranks for columns")
     val rankedUrl = calculator.rankByDay(accessRecords, URL_COL).persist()
     val rankedHost = calculator.rankByDay(accessRecords, VISITOR_COL).persist()

     log.info("Joining the two dataframes")
     calculator.joinDfsOnDayAndRank(rankedUrl, rankedHost, URL_COL, VISITOR_COL)
       .createOrReplaceTempView(FINAL_VIEW)

     /**
      * The previous statement registered a tempview called joined data. Use the calculater
      * to build the appropriate sql query based on the column names and the joinedData
      * view.
      *
      * We will issue this query with spark sql and show the results, un-truncated.
      */
     log.info("Issue query to {} to pull top {} per day", FINAL_VIEW, my_n)
     val finalResult = sqlContext.sql(
      calculator.buildSqlQuery(URL_COL, VISITOR_COL, FINAL_VIEW, my_n)
     )

     finalResult.show(finalResult.count().toInt, false)
     log.info("Program exiting successfully")
    } catch {
     case e: ConfigException =>
      log.error("No application.conf configuration found, aborting job. Exception:" + e.getMessage)
      println("No application.conf configuration found, aborting job. Exception:" + e.getMessage)
      System.exit(-1)
    } finally {
     if (session.isDefined) session.get.stop()
    }
}
