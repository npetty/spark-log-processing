import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import com.typesafe.config.ConfigFactory
import my.challenge.LogEntry.parseAccessLogEntry
import my.challenge.{LogEntry, TopNCalculator}
import org.scalatest.FunSuite

/**
 * Testing the functionality of the LogEntry class and object
 */
class LogEntryTest extends FunSuite
  with DataFrameSuiteBase
  with SharedSparkContext {

    val config = ConfigFactory.load("apptest.conf")
    val calculator = new TopNCalculator(config)

  /**
   * These are invalid lines found in the dataset. They should always evaluate to invalid
   */
    val invalidLines = Seq(
      "klothos.crl.research.digital.com - - [10/Jul/1995:16:45:50 -0400] \"\" 400 -",
      "firewall.dfw.ibm.com - - [20/Jul/1995:07:34:34 -0400] \"1/history/apollo/images/\" 400 -",
      "firewall.dfw.ibm.com - - [20/Jul/1995:07:53:24 -0400] \"1/history/apollo/images/\" 400 -",
      "128.159.122.20 - - [20/Jul/1995:15:28:50 -0400] \"k��tx��tG��t̓�\" 400 -",
      "128.159.122.20 - - [24/Jul/1995:13:52:50 -0400] \"k��tx��tG��t̓�\" 400 -",
      "alyssa.p",
      ""
    )

  /**
   * Test a valid line to make sure it is parsed appropriately. Probably could get a larger
   * sample for this, but this will server as a sanity check.
   */
    val validLine = Seq(
      """andreh.oz.net - - [01/Jul/1995:01:18:07 -0400] "GET /shuttle/technology/sts-newsref/sts-acronyms.html HTTP/1.0" 200 24570"""
    )

  /**
   * The valid line above should prodcue the following LogEntry object when parsed. Use this
   * in validation testing.
   */
    val expectedValidOutput = Seq(
      new LogEntry("andreh.oz.net","-", "-", "01/Jul/1995:01:18:07", "19950701", "GET", "/shuttle/technology/sts-newsref/sts-acronyms.html", 200, 24570)
    )

  /**
   * First test is a basic functionality test on a valid log entry line.
   */
    test("LogEntry parser should correctly parse valid lines"){

      //
      val logRddStr = spark.sparkContext.parallelize(validLine)
        .map(parseAccessLogEntry)
        //.filter(!_.visitor.equals(LogEntry.INVALID_CLIENT))
        .collect().mkString


      val expectedRddStr = spark.sparkContext.parallelize(expectedValidOutput)
          .collect().mkString

      println(logRddStr)
      println(expectedRddStr)

      assert(logRddStr.equalsIgnoreCase(expectedRddStr))
    }

  /**
   * Next test the known invalid lines to make sure they are given the default
   * visitor value of "Empty". Filter out lines with visitor = "Empty" and make
   * sure there are no entries left.
   */
    test("LogEntry parser should create an empty with host tag 'Empty' on invalid lines"){
      val invalidRdd = spark.sparkContext.parallelize(invalidLines)
        .map(parseAccessLogEntry)
        .filter(!_.visitor.equals(LogEntry.INVALID_CLIENT))

      assert(invalidRdd.count == 0)
    }
}
