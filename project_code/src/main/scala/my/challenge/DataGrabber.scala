package my.challenge

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkFiles}
import org.slf4j.{Logger, LoggerFactory}

/**
 * Convenience class for pulling remote data. It normally wouldn't be
 * needed as each method is simple and directly calls Spark API but
 * I found an issue with using the ftp url directly in sc.textFile
 * so needed a work around. This class is used to match the URL
 * and determine which method of file retrieval is needed.
 */
object DataGrabber {

  def log : Logger = LoggerFactory.getLogger( getClass )

  def getDataAsRdd(url: String, sc: SparkContext) : RDD[String] = {
    url match {
      case ftp if(url.startsWith("ftp")) => getDataAsRddFtp(url, sc)
      case other => getDataAsRddLocal(url, sc)
    }
  }

  /**
   * This uses an alternative method to sc.textFile as a workaround
   * to an issue with ftp URLs.
   *
   * @param url file path
   * @param sc SparkContext
   * @return RDD based on the input file
   */
  def getDataAsRddFtp(url: String, sc: SparkContext): RDD[String] = {
    log.info("Downloading external dataset at {}", url)
    sc.addFile(url)
    log.info("Data retrieved and textFile created")
    sc.textFile(SparkFiles.get(url.split("/").last))
  }

  /**
   * Standard method of grabbing data from a file
   *
   * @param url file path
   * @param sc SparkContext
   * @return RDD based on the input file
   */
  def getDataAsRddLocal(url: String, sc: SparkContext): RDD[String] = {
    log.info("Retrieving local dataset at {}", url)
    sc.textFile(url)
  }
}
