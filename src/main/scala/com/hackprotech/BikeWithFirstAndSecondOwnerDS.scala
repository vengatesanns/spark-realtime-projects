package com.hackprotech

import com.hackprotech.ConfigLoader.getSparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, SaveMode}

object BikeWithFirstAndSecondOwnerDS extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  case class Bike(model: String, price: Double, owner: String, age: Double, power: Double, brand: String)

  @transient lazy val logger = Logger.getLogger(getClass.getName)

  val bikeSchema = StructType(Array(
    StructField("model", StringType),
    StructField("price", DoubleType),
    StructField("city", StringType),
    StructField("kms_driven", DoubleType),
    StructField("owner", StringType),
    StructField("age", DoubleType),
    StructField("power", DoubleType),
    StructField("brand", StringType)
  ))

  val (sparkSession, config) = getSparkSession(args)

  val readPath = config.getString("inputPath");
  val writePath = config.getString("outputPath")

  import sparkSession.implicits._

  val bikeSourceDF: Dataset[Bike] = sparkSession.read
    .option("header", "true")
    .schema(bikeSchema)
    .csv(readPath).as[Bike].repartition(1)


  val resultDF = bikeSourceDF
    .select("model", "brand", "owner")
    .where(
      col("owner") =!= "Fourth Owner Or More"
        && col("age") <= 3
        && col("price").between(40000, 100000)
        && col("brand") === "Yamaha"
    )
    .groupBy(col("model"), col("brand"), col("owner")).count()

  resultDF.printSchema()
  resultDF.show(false)
  println(resultDF.count())
  resultDF.write.mode(SaveMode.Overwrite).csv(writePath)
  sparkSession.stop()

}
