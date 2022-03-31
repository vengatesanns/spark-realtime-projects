package com.hackprotech

import org.apache.spark.sql.SparkSession

import scala.io.StdIn.readLine

object SparkClusterWithStandaloneCluster {


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test").getOrCreate()

    val testRDD = spark.read.csv("Used_Bikes.csv")
//    testRDD.foreach(println)
//    readLine()

  }

}
