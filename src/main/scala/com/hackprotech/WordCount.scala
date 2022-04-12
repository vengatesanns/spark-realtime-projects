package com.hackprotech

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object WordCount extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = new SparkContext("local[10]", "WordCount")

  val sourceFileRDD = spark.textFile("src/main/resources/datasets/word_count.txt")

  //  val parsedRDD = sourceFileRDD.flatMap(lines => lines.split(" "))
  val parsedRDD = sourceFileRDD.flatMap(_.split(" "))

  //  val mappedRDD = parsedRDD.map(word => (word, 1))
  val mappedRDD = parsedRDD.map((_, 1))
  //  val finalResultRDD = mappedRDD.reduceByKey((v1, v2) => v1 + v2)
  val finalResultRDD = mappedRDD.reduceByKey(_ + _)
  finalResultRDD.collect().foreach(println)

}
