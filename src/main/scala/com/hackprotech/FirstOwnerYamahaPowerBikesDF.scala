package com.hackprotech

import com.hackprotech.ConfigLoader.getSparkConfig
import org.apache.log4j.Logger

object FirstOwnerYamahaPowerBikesDF extends App {

  @transient lazy val logger = Logger.getLogger(getClass.getName)

  val sparkSession = getSparkConfig

  val readPath = args(0)
  val writePath = args(1)

  val bikeSourceDF = sparkSession.read.option("header", "true").csv(readPath)
  bikeSourceDF.createOrReplaceTempView("used_bikes_tbl")

  val bikeSourceDF = sparkSession.sql("select * from used_bikes_tbl")

}
