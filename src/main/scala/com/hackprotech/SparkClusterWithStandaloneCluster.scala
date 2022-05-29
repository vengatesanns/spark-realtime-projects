package com.hackprotech

import org.apache.spark.sql.SparkSession

import java.net._

object SparkClusterWithStandaloneCluster {

  def getCurrentMachineIPAddress: String = {
    val localhost: InetAddress = InetAddress.getLocalHost
    localhost.getHostAddress
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test").getOrCreate()


    val testRDD = spark.sparkContext.parallelize(Range(1, 5)).repartition(5)
    val finalRDD = testRDD.map(item => s"Item - ${item}, IP-Address ${getCurrentMachineIPAddress}")

    finalRDD.saveAsTextFile("/home/bigdata/spark_op/")

    spark.stop()

  }

}
