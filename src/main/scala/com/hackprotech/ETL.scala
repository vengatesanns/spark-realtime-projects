package com.hackprotech

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}

object ETL extends App {
  // Spark SparkSession
  val sparkConf = new SparkConf()
  sparkConf.setAppName("ETL")
  sparkConf.setMaster("local")


  val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  sparkSession.sparkContext.setLogLevel("ERROR")


  val schema = StructType(Array(
    StructField("name", StringType),
    StructField("start_time", TimestampType),
    StructField("end_time", TimestampType),
    StructField("date", DateType)
  ))

//  case class Schema(name: String, )


  // Read the CSV file
  val sourceTVProgramsDF = sparkSession.read.option("header", true).schema(schema).csv("src/main/resources/programs.csv")

  // Partitioning By
  sourceTVProgramsDF.write.mode(SaveMode.Overwrite).partitionBy("date").csv("target/tv_programs")

//  sparkSession.read.option("header", true).schema(schema).csv("target/tv_programs").explain()
//  sparkSession.read.option("header", true).schema(schema).csv("target/tv_programs").filter(col("date") === "2022-05-19").explain()

  sourceTVProgramsDF.createOrReplaceTempView("tv_programs")

  sparkSession.sql("select expr(to_date(col()) from tv_programs").printSchema()


  val invalidRecordsDF = sparkSession.sql(
    """
      |with program_status as (
      |   select *,
      |    CASE WHEN start_time < lag(end_time) over(partition by date order by start_time asc)
      |    then 'INVALID' else 'VALID' end as status
      |    from tv_programs
      |  )
      |
      |    select * from program_status where status = 'INVALID'
      |""".stripMargin)

  invalidRecordsDF.show(false)



  //
  //
  //  // Get the count
  val resultCountDF = sourceTVProgramsDF
    .groupBy("date")
    .count()

  resultCountDF.show(false)
  //
  //
  //  // s3:// or gs://
  //  resultCountDF.write.csv("gs://bucket_name/tv_broadcast_details/")


}





