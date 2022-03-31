package com.hackprotech.railways

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.{SaveMode, SparkSession}

/*
* @author Vengatesan Nagarajan
*
* To flattening the Source JSON file to target ORC file using Spark DataFrame API
* */
object RailwaysDataFlatteningUsingDF {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val ROOT_FOLDER = "/home/vengat/big_data/projects/spark_jars"
  //    val ROOT_FOLDER = "src/main/resources"

  def main(args: Array[String]): Unit = {

    //    Create SparkConf
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local")
    sparkConf.setAppName("RailwaysDataSeggregationDF")

    //    Create Spark Session
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    def flatteningTrainsJson() = {
      //     Read schedules.json file using spark session
      val sourceTrainsDF = spark.read.json(s"$ROOT_FOLDER/datasets/irctc_railways/trains.json")

      val explodedSourceTrainDF = sourceTrainsDF
        .select(explode(col("features")).as("record"))

      val extractedTrainsDF = explodedSourceTrainDF
        .select(col("record.geometry.coordinates"),
          col("record.properties.arrival"),
          col("record.properties.from_station_code"),
          col("record.properties.name").as("train_name"),
          col("record.properties.zone"),
          col("record.properties.from_station_name"),
          col("record.properties.departure"),
          col("record.properties.number").as("train_no"),
          col("record.properties.return_train"),
          col("record.properties.to_station_code"),
          col("record.properties.to_station_name"),
          col("record.properties.duration_h").as("duration_hrs"),
          col("record.properties.type"),
          col("record.properties.distance"))

      extractedTrainsDF.printSchema()
      extractedTrainsDF.show(false)
      extractedTrainsDF
    }

    def flatteningStationJson() = {
      //    Read stations.json file using spark session
      val stationDF = spark.read.json(s"$ROOT_FOLDER/datasets/irctc_railways/stations.json")

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
      extractedStationDF
    }

    def flatteningSchedulesJson() = {
      spark.read.json(s"$ROOT_FOLDER/datasets/irctc_railways/schedules.json")
    }

    try {

      val flattenedSchedulesDF = flatteningSchedulesJson()
      //    Persist the schedules details as orc file
      flattenedSchedulesDF.write.mode(SaveMode.Overwrite).orc(s"$ROOT_FOLDER/target/schedules/")
      println("Schedule details persisted successfully!")

      val flattenedTrainsDF = flatteningTrainsJson()
      //    Persist the train details as orc file
      flattenedTrainsDF.write.mode(SaveMode.Overwrite).orc(s"$ROOT_FOLDER/target/trains/")
      println("Train details persisted successfully!")

      val flattenedStationDF = flatteningStationJson()
      //    Persist the station details as orc file
      flattenedStationDF.write.mode(SaveMode.Overwrite).orc(s"$ROOT_FOLDER/target/stations/")
      println("Station details persisted successfully!")
    }
    catch {
      case exception: Exception => {
        println(s"Flattening the source files failed ${exception}")
        throw exception
      }
    }
    finally {
      spark.stop()
    }

  }

}
