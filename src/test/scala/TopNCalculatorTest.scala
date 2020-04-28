import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import com.typesafe.config.ConfigFactory
import my.challenge.{LogEntry, TopNCalculator}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.{FunSuite, Matchers}

/**
 * Testing the functionality of the TopNCalculator class
 */
class TopNCalculatorTest extends FunSuite
  with DataFrameSuiteBase
  with SharedSparkContext
  with Matchers{

  /**
   * Grap the apptest.conf file from src/test/resources and build a calculator object
   */
  val config = ConfigFactory.load("apptest.conf")
  val calculator = new TopNCalculator(config)

  /**
   * Define an array of LogEntry objects to test valid functionality.
   */
  val logsRankByDay = Array(
    new LogEntry("10.10.10.1","-", "-", "[01/Jul/1995:00:00:00 -0400]", "19950701", "GET", "/test1.html", -1, -1),
    new LogEntry("10.10.10.1","-", "-", "[01/Jul/1995:00:00:01 -0400]", "19950701", "GET", "/test1.html", -1, -1),
    new LogEntry("10.10.10.1","-", "-", "[01/Jul/1995:00:00:02 -0400]", "19950701", "GET", "/test2.html", -1, -1),
    new LogEntry("10.10.10.1","-", "-", "[01/Jul/1995:00:00:03 -0400]", "19950701", "GET", "/test3.html", -1, -1),
    new LogEntry("10.10.10.2","-", "-", "[01/Jul/1995:00:00:04 -0400]", "19950701", "GET", "/test1.html", -1, -1),
    new LogEntry("10.10.10.2","-", "-", "[01/Jul/1995:00:00:05 -0400]", "19950701", "GET", "/test1.html", -1, -1),
    new LogEntry("10.10.10.2","-", "-", "[01/Jul/1995:00:00:06 -0400]", "19950701", "GET", "/test2.html", -1, -1),
    new LogEntry("10.10.10.3","-", "-", "[01/Jul/1995:00:00:07 -0400]", "19950701", "GET", "/test1.html", -1, -1),
    new LogEntry("10.10.10.3","-", "-", "[01/Jul/1995:00:00:08 -0400]", "19950701", "GET", "/test4.html", -1, -1),
    new LogEntry("10.10.10.4","-", "-", "[01/Jul/1995:00:00:09 -0400]", "19950701", "GET", "/test5.html", -1, -1),
    new LogEntry("10.10.10.5","-", "-", "[01/Jul/1995:00:00:10 -0400]", "19950701", "GET", "/test6.html", -1, -1),
    new LogEntry("10.10.10.6","-", "-", "[01/Jul/1995:00:00:11 -0400]", "19950701", "GET", "/test6.html", -1, -1),
    new LogEntry("10.10.10.6","-", "-", "[02/Jul/1995:00:00:11 -0400]", "19950702", "GET", "/test3.html", -1, -1)
  )

  /**
   * First test to check valid functionality. Given the static set of entries above, we know the exptected
   * output should group and produce 7 rows per new dataframe.
   */
  test("rankByDay should return a DF with row count equal to distinct values of the ranked column per day."){
    val logRdd = spark.sparkContext.parallelize(logsRankByDay)
    val logDf = spark.sqlContext.createDataFrame(logRdd)

    assert(calculator.rankByDay(logDf, "url").count() == 7, "Count of ranked URLs should be 6")
    assert(calculator.rankByDay(logDf, "visitor").count() == 7, "Count of ranked visitors should be 6")
  }

  /**
   * It should rank the most frequently occurring entry for each day as 1
   */
  test("rankByDay should give daily_rank of 1 to most frequent group member."){
    val logRdd = spark.sparkContext.parallelize(logsRankByDay)
    val logDf = spark.sqlContext.createDataFrame(logRdd)

    val topRank = calculator.rankByDay(logDf, "url")
      .filter("daily_rank == 1")
      .orderBy("day")
      .select("url")
      .collect()
      .map(_.get(0)).mkString(",")

    assert(topRank.equals("/test1.html,/test3.html"),"Top ranked url was "+topRank+", but should be '/test1.html,/test3.html,")
  }

  /**
   * Define static ranked data sets to test the joining
   */
  val rankedDf1 = Array(
    Row("19950701","/test1.html", 300, 1),
    Row("19950701","/test2.html", 200, 2),
    Row("19950701","/test3.html", 100, 3),
    Row("19950702","/test1.html", 200, 1),
    Row("19950702","/test2.html", 100, 2)
  )
  val rankedDf2 = Array(
    Row("19950701","10.10.10.1", 30, 1),
    Row("19950701","10.10.10.2", 20, 2),
    Row("19950701","10.10.10.3", 10, 3),
    Row("19950702","10.10.10.1", 20, 1),
    Row("19950702","10.10.10.2", 10, 2)
  )

  /**
   * This is the expected joined output.
   */
  val joinOutput = Array(
    Row("19950701",1,"/test1.html", 300,"10.10.10.1", 30),
    Row("19950701",2,"/test2.html", 200,"10.10.10.2", 20),
    Row("19950701",3,"/test3.html", 100,"10.10.10.3", 10),
    Row("19950702",1,"/test1.html", 200,"10.10.10.1", 20),
    Row("19950702",2,"/test2.html", 100,"10.10.10.2", 10)
  )

  /**
   * Now run the test to see if the expected output above matches the calculated output
   * from our TopNCalculator class
   */
  test("joinDfsOnDayAndRank should join to DFs on day and rank"){
    val urlField = StructField("url", StringType, nullable = true)
    val urlCountField = StructField("url_count", IntegerType, nullable = true)
    val visField = new StructField("visitor", StringType, nullable = true)
    val visCountField = new StructField("visitor_count", IntegerType, nullable = true)
    val countField = new StructField("count", IntegerType, nullable = true)
    val dayField = new StructField("day", StringType, nullable = true)
    val dailyRankField = new StructField("daily_rank", IntegerType, nullable = true)

    val df1Schema = StructType(Array(dayField,urlField,countField,dailyRankField))
    val df2Schema = StructType(Array(dayField,visField,countField,dailyRankField))
    val outputSchema = StructType(Array(dayField,dailyRankField,urlField,urlCountField,visField,visCountField))

    val df1 = sqlContext.createDataFrame(spark.sparkContext.parallelize(rankedDf1), df1Schema)
    val df2 = sqlContext.createDataFrame(spark.sparkContext.parallelize(rankedDf2), df2Schema)
    val expectedOutput = sqlContext.createDataFrame(spark.sparkContext.parallelize(joinOutput), outputSchema)

    val calculatedOutput = calculator.joinDfsOnDayAndRank(df1, df2, "url", "visitor")
      .orderBy("day", "daily_rank")

    df1.show()
    df2.show()
    expectedOutput.show()
    calculatedOutput.show()

    assertDataFrameEquals(expectedOutput, calculatedOutput)
  }

  /**
   * Test the sql generator function against the expected output for a known input.
   */
  val expectedSqlOutput = "select day, daily_rank, url, url_count, visitors, visitors_count "+
    "from joinedData where daily_rank <= 10 order by day, daily_rank"
  test("buildSqlQuery should return expected SQL query given inputs"){
    assert(expectedSqlOutput.equals(calculator.buildSqlQuery("url", "visitors", "joinedData", 10)))
  }
}

