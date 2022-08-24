package com.hackprotech.customer

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object CustomerETL extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.setMaster("local")
  sparkConf.setAppName("CustomerETL")

  val spark = SparkSession.builder().config(sparkConf).getOrCreate()

  val options = Map(
    "header" -> "true"
  )

  val customerRawDF = spark.read.options(options).csv("src/main/resources/customer/customer_raw.csv")
  customerRawDF.createOrReplaceTempView("customer_raw")

  val customerTableDF = spark.read.options(options).csv("src/main/resources/customer/customer_table.csv")
  customerTableDF.createOrReplaceTempView("customer_table")


  spark.sql(
    """
      |select *,
      | CASE
      |    WHEN ct.phone_number != cr.phone_number then 'UPDATE'
      |    WHEN ct.phone_number is null then 'INSERT'
      |    WHEN cr.phone_number is null then 'DELETE'
      | END
      |from customer_table ct
      | full outer join customer_raw cr on cr.customer_id = ct.customer_id
      |""".stripMargin).show(false)

}
