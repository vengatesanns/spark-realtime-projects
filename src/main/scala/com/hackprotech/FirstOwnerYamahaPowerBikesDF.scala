package com.hackprotech

import com.hackprotech.ConfigLoader.getSparkSession
import org.apache.log4j.Logger
import org.apache.spark.sql.SaveMode

object FirstOwnerYamahaPowerBikesDF extends App {

  @transient lazy val logger = Logger.getLogger(getClass.getName)

  val (sparkSession, config) = getSparkSession

  val readPath = config.getString("inputPath")
  val writePath = config.getString("outputPath")

  val bikeSourceDF = sparkSession.read.option("header", "true").csv(readPath)
  bikeSourceDF.createOrReplaceTempView("used_bikes_tbl")

  val bikeSourceDF1 = sparkSession.sql("select * from used_bikes_tbl")
  bikeSourceDF1.show(false)
  bikeSourceDF1.write.mode(SaveMode.Append).csv(writePath)

}
