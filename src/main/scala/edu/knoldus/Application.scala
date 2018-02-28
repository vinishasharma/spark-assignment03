package edu.knoldus

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.log4j.Logger
import org.apache.log4j.Level


object Application extends App {
  val operation = new Operation
  Logger.getLogger("org").setLevel(Level.OFF)
  val conf = new SparkConf().setMaster("local").setAppName("spark sql")
  val filePath = "/home/knoldus/IdeaProjects/spark-assignment03/src/main/resources/D1.csv"

  //Q.1
  val FootballTeamDataFrame: DataFrame = operation.getFootballDataFrame(filePath)
  FootballTeamDataFrame.createOrReplaceTempView("football")

  //Q.2
  val homeTeamCount = operation.getHomeTeamCount()
  homeTeamCount.show()

  //Q.3
  val winningPercentageSQLDataFrame = operation.getWinningPercentage()
  winningPercentageSQLDataFrame.show()

  //Q-4
  val footballDataSet = operation.getFootballDataSet(FootballTeamDataFrame)

  //Q-5
  val footballMatchesCount: DataFrame = operation.getTotalMatchPerTeam(footballDataSet)
  footballMatchesCount.show(false)

  //Q-6
  val topTenWinningTeams = operation.getTopTenWinningTeams(footballDataSet)
  topTenWinningTeams.show(false)

}
