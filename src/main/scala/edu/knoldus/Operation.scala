package edu.knoldus

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import org.apache.log4j.Level


object Operation extends App {

  Logger.getLogger("org").setLevel(Level.OFF)
  val conf = new SparkConf().setMaster("local").setAppName("spark sql")

  val spark = SparkSession
    .builder()
    .appName("spark sql")
    .config(conf)
    .getOrCreate()

  import spark.implicits._

  //Q.1
  val FootballTeamDataFrame: DataFrame = spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv("/home/knoldus/IdeaProjects/spark-assignment03/src/main/resources/D1.csv")
  FootballTeamDataFrame.createOrReplaceTempView("football")

  //Q.2
  val homeTeamCount = spark.sql("SELECT HomeTeam, count(HomeTeam) As Counts FROM football GROUP BY HomeTeam")
  homeTeamCount.createOrReplaceTempView("HomeTeamMatches")
  homeTeamCount.show()

  //Q.3
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
    "ON TotalTeamWins.Team=TotalTeamMatches.Team ORDER BY Percentage DESC LIMIT 10").show()

  //Q-4
  val footballDataSet = FootballTeamDataFrame.map(
    row => Football(row.getString(2), row.getString(3),
      row.getInt(4), row.getInt(5), row.getString(6)))
  
  //Q-5
  val footballMatchesCount: DataFrame = footballDataSet.select("HomeTeam").withColumnRenamed("HomeTeam", "Team")
    .union(footballDataSet.select("AwayTeam").withColumnRenamed("AwayTeam", "Team")).groupBy("Team").count()
  footballMatchesCount.show(false)

  //Q-6
  val homeDataFrame = footballDataSet.select("HomeTeam", "FTR").where("FTR = 'H'").groupBy("HomeTeam").count().withColumnRenamed("count", "HomeWins")
  val awayTeamDataFrame = footballDataSet.select("AwayTeam", "FTR").where("FTR = 'A'").groupBy("AwayTeam").count().withColumnRenamed("count", "AwayWins")
  val teamsDataFrame = homeDataFrame.join(awayTeamDataFrame, homeDataFrame.col("HomeTeam") === awayTeamDataFrame.col("AwayTeam"))
  val addMatches: (Int, Int) => Int = (HomeMatches: Int, TeamMatches: Int) => HomeMatches + TeamMatches
  val total = udf(addMatches)
  teamsDataFrame.withColumn("TotalWins", total(col("HomeWins"), col("AwayWins"))).select("HomeTeam", "TotalWins")
    .withColumnRenamed("HomeTeam", "Team").sort(desc("TotalWins")).limit(10).show(false)


}
