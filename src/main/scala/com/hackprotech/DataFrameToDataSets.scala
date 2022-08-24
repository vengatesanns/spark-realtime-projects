package com.hackprotech

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}


object DataFrameToDataSets {

  Logger.getLogger("org").setLevel(Level.ERROR)

  case class Bike(bike_name: String, price: Double, city: String, kms_driven: Double, owner: String, age: Double, power: Double, brand: String)
  
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setAppName("DFtoDS")
    sparkConf.setMaster("local")

    val bikeSchema = StructType(Array(
      StructField("bike_name", StringType),
      StructField("price", DoubleType),
      StructField("city", StringType),
      StructField("kms_driven", DoubleType),
      StructField("owner", StringType),
      StructField("age", DoubleType),
      StructField("power", DoubleType),
      StructField("brand", StringType)
    ))

    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val sourceDataframe = sparkSession.read.option("header", true).schema(bikeSchema).csv("src/main/resources/datasets/Used_Bikes.csv")

    sourceDataframe.show(false)


    import sparkSession.implicits._
    val convertedDataSets = sourceDataframe.as[Bike]

    convertedDataSets.show(false)

    convertedDataSets.map(bike => bike.bike_name.toUpperCase()).show(false)


  }

}
