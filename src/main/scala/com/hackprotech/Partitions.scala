package com.hackprotech

import org.apache.spark.sql.SparkSession

object Partitions extends App {

  val spark = SparkSession.builder().appName("Sample")
    .master("local[10]").getOrCreate()

  val sourceRDD = spark.sparkContext
    //    .parallelize(Seq(1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000, 11000, 12000))
    .parallelize(Range(0, 10))

  sourceRDD.saveAsTextFile("sample_op")

}
