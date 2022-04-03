package com.hackprotech.railways

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{broadcast, col}

/*
* @author Vengatesan Nagarajan
*
* To Compare the train, schedules and station to derive the final train schedules sheet
* as in CSV format using Spark DataFrame API
* */
object RailwayScheduleDetailsDF {

  Logger.getLogger("org").setLevel(Level.ERROR)

  //  val ROOT_FOLDER = "/home/vengat/big_data/projects/spark_jars"
  val ROOT_FOLDER = "src/main/resources"

  val sparkConf = new SparkConf()
  sparkConf.setAppName("Railway_Schedule_Details_DF")
  sparkConf.setMaster("local")

  val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  def main(args: Array[String]): Unit = {

    val sourceSchedulesDF = spark.read.orc(s"${ROOT_FOLDER}/target/schedules")

    val selectedColsSrcSchedulesDF = sourceSchedulesDF
      .select("departure", "id", "station_code", "train_number")
    //      .where(col("train_number") === 310956)

    println("Source Schedules => files read and selected columns fetched successfully!!!")

    //     Station Details
    val sourceStationDetailsDF = spark.read.orc(s"${ROOT_FOLDER}/target/stations")

    val selectedColsSrcStationDetailsDF = sourceStationDetailsDF
      .select("state", "station_code", "name", "address")

    println("Source Stations => files read and selected columns fetched successfully!!!")

    //    Broadcast Variables for station details
    val trainSchedulesWithStateDetails = joinSchedulesAndStationDetails(selectedColsSrcSchedulesDF, selectedColsSrcStationDetailsDF)

    println("Source Schedules and Station joined through broadcast as a lookup done!!!")

    //     Train Details
    val finalTrainDetails = parseTrainDetails(trainSchedulesWithStateDetails)

    println("Final train and schedule related details processed successfully!!!")

    finalTrainDetails.write.mode(SaveMode.Overwrite)
      //      .partitionBy("state")
      .option("header", "true")
      .csv(s"${ROOT_FOLDER}/target/actual_schedules")

    println("Final train schedule details persisted successfully as CSV format and partitioned by state!!!")

  }

  //  Join Schedules and Station Details
  def joinSchedulesAndStationDetails(selectedColsSrcSchedulesDF: DataFrame,
                                     selectedColsSrcStationDetailsDF: DataFrame): DataFrame = {
    selectedColsSrcSchedulesDF.join(
      broadcast(selectedColsSrcStationDetailsDF),
      Seq("station_code"))
      .withColumnRenamed("name", "train_name")
      .drop(selectedColsSrcStationDetailsDF("station_code"))
  }


  //  Parse trainDetails
  def parseTrainDetails(trainSchedulesWithStateDetails: DataFrame): Dataset[Row] = {
    val trainDF = spark.read.orc(s"${ROOT_FOLDER}/target/trains")
    val cols = List("arrival", "departure", "id", "station_code", "train_number",
      "from_station_name", "to_station_name", "duration_hrs", "distance", "state")
      .map(column => col(column))

    //    Resultant DF (ready to persist)
    trainSchedulesWithStateDetails
      .join(trainDF, trainDF("train_no") === trainSchedulesWithStateDetails("train_number"), "INNER")
      .drop(trainDF("departure"))
      .select(cols: _*)
  }

}
