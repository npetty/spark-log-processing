package my.challenge

import com.typesafe.config.Config
//import my.challenge.TopN.{accessRecords, sqlContext}
//import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{desc, rank}

/**
 *
 * @param config Object containing application configuration, specifically for spark
 *               details, file url, and N value.
 */
class TopNCalculator(config: Config) {

  val GROUPING_COLUMN = "day"
  val COUNT_COL = "count"
  val RANK_COLUMN = "daily_rank"

  val w = Window.partitionBy(GROUPING_COLUMN).orderBy(desc(COUNT_COL))

  /**
   *
   * @param inputDf This accepts a dataframe of LogEntry objects.
   * @param countCol Name of column to group-by and count
   * @return Dataframe grouped on day and countCol with counts applied
   *         per group.
   *         Columns include "day", <countCol>, "count", "daily_rank"
   */
  def rankByDay(inputDf: DataFrame, countCol: String) : DataFrame = {
    inputDf.groupBy(GROUPING_COLUMN, countCol)
      .count()
      .withColumn(RANK_COLUMN, rank().over(w))
  }

  /**
   * Joins the two raned dataframes into a single dataframe for more concise
   * display and distribution.
   *
   * @param df1 First dataframe. Normal behaviour is to use the output from
   *            rankByDay as input here. Input format should be
   *            "day", <col1>, "count", "daily_rank"
   * @param df2 Second dataframe for joining. Same format as above
   * @param col1 Name of column for joining in first dataframe
   * @param col2 Name of column for joining in second dataframe
   * @return A dataframe resulting from the join of the two input DF's on the
   *         day and daily_rank columns. Count columns will be renamed
   *         Output format:
   *         "day", "daily_rank" col1, col1_count, col2, col2_count
   */
  def joinDfsOnDayAndRank(df1: DataFrame, df2: DataFrame, col1: String, col2: String) : DataFrame = {
    df1.selectExpr(GROUPING_COLUMN, RANK_COLUMN, col1, s"%s as %s_%s".format(COUNT_COL, col1, COUNT_COL))
      .join(df2.selectExpr(GROUPING_COLUMN, RANK_COLUMN, col2, s"%s as %s_%s".format(COUNT_COL, col2, COUNT_COL)),
      Seq(GROUPING_COLUMN, RANK_COLUMN),"outer")
  }

  /**
   * Build the sql query to issue to the joined dataframe from joinDfsOnDayAndRank. This
   * query will filter dailiy rank to the supplied value of N.
   *
   * @param col1 name of one of the ranked columns
   * @param col2 name of the second ranked column
   * @param view name of the registred temp view that will be queried
   * @param n how many top-n to display per day.
   * @return A dataframe with the final output display top-n visitors, urls.
   *
   * Output format:
   *  day", "daily_rank" col1, col1_count, col2, col2_count
   *
   *  Certain url/visitor values and counts will display null by design. The
   *  rank function is used and not dense rank, so if there is a tie in count
   *  not every rank value will be used. This results in some null entries
   *  in the join. This is not an error.
   */
  def buildSqlQuery(col1: String, col2: String, view: String, n: Int) = {
    "select " +
      GROUPING_COLUMN + ", "+RANK_COLUMN+", " +
      col1+", "+col1+"_"+COUNT_COL+", "+
      col2+", "+col2+"_"+COUNT_COL+" "+
      "from "+view+" "+
      "where "+RANK_COLUMN+" <= " + n + " " +  //<- apply variable N to query
      "order by "+GROUPING_COLUMN+", "+RANK_COLUMN
  }
}
