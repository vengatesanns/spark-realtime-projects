package com.hackprotech


import com.hackprotech.ConfigLoader.getSparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.{col, spark_partition_id}
import org.apache.spark.sql.types._

object PredicatePushDown extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

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
  val targetBikeSchema = StructType(Array(
    StructField("model", StringType),
    StructField("owner", StringType),
    StructField("count", IntegerType),
    StructField("brand", StringType)
  ))

  val (sparkSession, config) = getSparkSession(args)

  val readPath = config.getString("inputPath");
  val writePath = config.getString("outputPath")

  val bikeSourceDF = sparkSession.read
    .option("header", "true")
    .schema(bikeSchema)
    .csv(readPath)


  val resultDF = bikeSourceDF
    .select("model", "brand", "owner")
    .where(
      col("owner") =!= "Fourth Owner Or More"
        && col("age") <= 3
        && col("price").between(40000, 100000)
      //        && col("brand") === "Yamaha"
    )
    .groupBy(col("model"), col("brand"), col("owner")).count()

  resultDF.printSchema()
  resultDF.show(false)

  //  Partitions
  println(resultDF.count())
  println(resultDF.rdd.getNumPartitions)
  resultDF.groupBy(spark_partition_id()).count().show(false)

  //  Partition Pruning
  resultDF.write.mode(SaveMode.Overwrite)
    .partitionBy("brand")
    .csv(writePath)

  //  Predicate Pushed Down
  val sourceDF = sparkSession.read.schema(targetBikeSchema).csv(writePath)
    .filter(col("brand") === "Honda")
    .filter(col("model").startsWith("Honda CB"))
  sourceDF.explain
  sourceDF.show(false)


  sparkSession.stop()

}

