package com.hackprotech

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Partitions extends App {

  val sparkConf = new SparkConf()
  sparkConf.setAppName("Partitions")
  sparkConf.setMaster("local[100]")

  val spark = SparkSession.builder().config(sparkConf).getOrCreate()

  val numbersRDD = spark.sparkContext.parallelize(Range(0, 10))

  println(numbersRDD.getNumPartitions)
  numbersRDD.saveAsTextFile("target/sample_op")

}
