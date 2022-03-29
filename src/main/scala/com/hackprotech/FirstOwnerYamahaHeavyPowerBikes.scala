/*
package com.hackprotech

import com.hackprotech.ConfigLoader.getSparkSession
import org.apache.log4j.Logger


object FirstOwnerYamahaHeavyPowerBikes extends App {

  @transient lazy val logger = Logger.getLogger(getClass.getName)

  val sparkContext = getSparkSession

  val sourceRDD = sparkContext.textFile(readPath)
  val bikesRDD = sourceRDD.map(line => line.split(",").map(bike => bike.trim))

  val yamahaBikesRDD = bikesRDD
    .filter(bike => bike(7).equalsIgnoreCase("Yamaha") && bike(6).toDouble > 150)
    .map(bike => (bike(0), bike(1)))
  yamahaBikesRDD.foreach(s => println(s._1, s._2))
  logger.info(yamahaBikesRDD.collect().map(bike => s" Bike Name - ${bike._1}, Price - ${bike._2}").mkString(" \n "))

}

*/
