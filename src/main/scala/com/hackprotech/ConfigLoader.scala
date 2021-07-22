package com.hackprotech

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ConfigLoader {

  def getSparkConfig: Config = {
    val config = ConfigFactory.load()
    config
  }

  def getSparkSession: (SparkSession, Config) = {
    val sparkConf: SparkConf = new SparkConf()
    val config: Config = getSparkConfig
    config.getConfig("sparkOptions").entrySet().forEach(sparkOpts => {
      sparkConf.set(sparkOpts.getKey, sparkOpts.getValue.render().replace("\"", ""))
    })
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    config.getConfig("hadoopOptions").entrySet().forEach(hadoopOpts => {
      sparkSession.sparkContext.hadoopConfiguration.set(hadoopOpts.getKey, hadoopOpts.getValue.render().replace("\"", ""))
    })
    (sparkSession, config)
  }
}