package edu.knoldus

import org.apache.spark.{SparkConf, sql}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import org.apache.log4j.Level

class Operation {

  Logger.getLogger("org").setLevel(Level.OFF)
  val conf = new SparkConf().setMaster("local").setAppName("spark sql")

  val spark = SparkSession
    .builder()
    .appName("spark sql")
    .config(conf)
    .getOrCreate()

  import spark.implicits._

  /**
    * this method gets total football teams
    * dat as DataFrame
    *
    * @param filePath
    * @return
    */
  def getFootballDataFrame(filePath: String): DataFrame = {
    spark.read.option("header", "true")
      .option("inferSchema", "true")
      .csv(filePath)
  }

  /**
    * this method gets total
    * home team count
    *
    * @return
    */
  def getHomeTeamCount(): sql.DataFrame = {
    spark.sql("SELECT HomeTeam, count(HomeTeam) As Counts FROM football GROUP BY HomeTeam")
  }

  /**
    * this methos gives
    * winning percentages of top
    * 10 teams
    *
    * @return
    */
  def getWinningPercentage(): sql.DataFrame = {
    val homeTeamCount = spark.sql("SELECT HomeTeam, count(HomeTeam) As Counts FROM football GROUP BY HomeTeam")
    homeTeamCount.createOrReplaceTempView("HomeTeamMatches")
    spark.sql("SELECT AwayTeam, count(AwayTeam) As Counts FROM football GROUP BY AwayTeam")
      .createOrReplaceTempView("MatchesAsAwayTeam")

    spark.sql("SELECT HomeTeam, count(HomeTeam) As Wins FROM football WHERE FTR = 'H' GROUP BY HomeTeam")
      .createOrReplaceTempView("HomeTeamWins")
    spark.sql("SELECT AwayTeam, count(AwayTeam) As Wins FROM football WHERE FTR = 'A' GROUP BY AwayTeam")
      .createOrReplaceTempView("AwayTeamWins")

    spark.sql("SELECT HomeTeam As Team, (HomeTeamWins.Wins + AwayTeamWins.Wins) AS Wins FROM HomeTeamWins FULL OUTER JOIN AwayTeamWins " +
      "ON HomeTeamWins.HomeTeam=AwayTeamWins.AwayTeam ").createOrReplaceTempView("TotalTeamWins")

    spark.sql("SELECT HomeTeam As Team, (HomeTeamMatches.Counts + MatchesAsAwayTeam.Counts) AS Total FROM HomeTeamMatches " +
      "FULL OUTER JOIN MatchesAsAwayTeam " +
      "ON HomeTeamMatches.HomeTeam=MatchesAsAwayTeam.AwayTeam ").createOrReplaceTempView("TotalTeamMatches")

    spark.sql("SELECT TotalTeamMatches.Team, (Wins/Total)*100 as Percentage FROM TotalTeamMatches FULL OUTER JOIN TotalTeamWins " +
      "ON TotalTeamWins.Team=TotalTeamMatches.Team ORDER BY Percentage DESC LIMIT 10")
  }

  def getFootballDataSet(footballDataFrame: DataFrame): Dataset[Football] = {
    footballDataFrame.map(
      row => Football(row.getString(2), row.getString(3),
        row.getInt(4), row.getInt(5), row.getString(6)))
  }

  /**
    * this method gives total
    * matches per team
    *
    * @param footballDataSet
    * @return
    */
  def getTotalMatchPerTeam(footballDataSet: Dataset[Football]): DataFrame = {
    footballDataSet.select("HomeTeam").withColumnRenamed("HomeTeam", "Team")
      .union(footballDataSet.select("AwayTeam").withColumnRenamed("AwayTeam", "Team")).groupBy("Team").count()
  }

  /**
    * this method gives
    * top ten winning teams
    *
    * @param footballDataSet
    * @return
    */
  def getTopTenWinningTeams(footballDataSet: Dataset[Football]): Dataset[Row] = {
    val homeDataFrame = footballDataSet.select("HomeTeam", "FTR").where("FTR = 'H'").groupBy("HomeTeam").count().withColumnRenamed("count", "HomeWins")
    val awayTeamDataFrame = footballDataSet.select("AwayTeam", "FTR").where("FTR = 'A'").groupBy("AwayTeam").count().withColumnRenamed("count", "AwayWins")
    val teamsDataFrame = homeDataFrame.join(awayTeamDataFrame, homeDataFrame.col("HomeTeam") === awayTeamDataFrame.col("AwayTeam"))
    val addMatches: (Int, Int) => Int = (HomeMatches: Int, TeamMatches: Int) => HomeMatches + TeamMatches
    val total = udf(addMatches)
    teamsDataFrame.withColumn("TotalWins", total(col("HomeWins"), col("AwayWins"))).select("HomeTeam", "TotalWins")
      .withColumnRenamed("HomeTeam", "Team").sort(desc("TotalWins")).limit(10)
  }
}
