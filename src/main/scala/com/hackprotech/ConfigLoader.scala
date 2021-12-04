package com.hackprotech

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @author Vengat
 */
object ConfigLoader {

  def getSparkConfig(args: Array[String]): Config = {
    val properties = scala.collection.mutable.Map[String, String]()
    args.foreach(arg => {
      val jvmArgs = arg.split("=")
      // TODO Need to check other Possibilities
      val key = jvmArgs(0).replace("-D", "")
      val value = jvmArgs(1).toString
      properties += key -> value
    })
    val config = ConfigFactory.load(properties("configFile"))
    config
  }

  def getSparkSession(args: Array[String]): (SparkSession, Config) = {
    val sparkConf: SparkConf = new SparkConf()
    val config: Config = getSparkConfig(args)
    config.getConfig("sparkConf").entrySet().forEach(sparkOpts => {
      sparkConf.set(sparkOpts.getKey, sparkOpts.getValue.render().replace("\"", ""))
    })
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    config.getConfig("hadoopConf").entrySet().forEach(hadoopOpts => {
      sparkSession.sparkContext.hadoopConfiguration.set(hadoopOpts.getKey, hadoopOpts.getValue.render().replace("\"", ""))
    })
    (sparkSession, config)
  }
}