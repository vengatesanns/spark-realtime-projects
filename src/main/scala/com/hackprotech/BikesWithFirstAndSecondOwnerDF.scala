package com.hackprotech

import com.hackprotech.ConfigLoader.getSparkSession
import org.apache.log4j.Logger
import org.apache.spark.sql.SaveMode

/**
 * @author Vengat
 *
 */
object BikesWithFirstAndSecondOwnerDF extends App {

  @transient lazy val logger = Logger.getLogger(getClass.getName)

  val (sparkSession, config) = getSparkSession(args)

  val readPath = config.getString("inputPath");
  val writePath = config.getString("outputPath")

  val bikeSourceDF = sparkSession.read.option("header", "true").csv(readPath)
  bikeSourceDF.createOrReplaceTempView("used_bikes_tbl")

  val resultDF = sparkSession.sql(
    """
      |select bike_name, brand, count(*) as count from used_bikes_tbl
      |where owner in ('First Owner', 'Second Owner')
      |and age = 3
      |and price between 50000 and 100000
      |and brand = 'Yamaha'
      |group by bike_name, brand
      |""".stripMargin)


  resultDF.show(false)
  resultDF.write.mode(SaveMode.Overwrite).csv(writePath)
  sparkSession.stop()

}
