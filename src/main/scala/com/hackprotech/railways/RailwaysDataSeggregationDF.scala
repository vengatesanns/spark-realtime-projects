package com.hackprotech.railways

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.{SaveMode, SparkSession}

object RailwaysDataSeggregationDF {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {

    //    Create SparkConf
    val sparkConf = new SparkConf()
    //    sparkConf.setMaster("local")
    sparkConf.setAppName("RailwaysDataSeggregationDF")

    //    Create Spark Session
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    //     Read schedules.json file using spark session
    val sourceSchedulesDF = spark.read.json("src/main/resources/datasets/irctc_railways/schedules.json")
    sourceSchedulesDF.show(false)

    //    Read stations.json file using spark session
    val stationDF = spark.read.json("src/main/resources/datasets/irctc_railways/stations.json")

    //    Explode the feature attribute from json data and create a new rows for each json object
    val coordinatesAndPropertiesDF = stationDF
      .select(explode(col("features")).as("record"))

    //    Extract the specific columns from json attributes
    val extractedStationDF = coordinatesAndPropertiesDF
      .select(col("record.geometry.coordinates"),
        col("record.properties.state"),
        col("record.properties.code").as("station_code"),
        col("record.properties.name"),
        col("record.properties.zone"),
        col("record.properties.address"))


    extractedStationDF.printSchema()
    extractedStationDF.show(truncate = false)
    extractedStationDF.write.mode(SaveMode.Overwrite).orc("target/station/")
  }

}
