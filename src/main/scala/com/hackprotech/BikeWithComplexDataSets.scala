package com.hackprotech

import com.hackprotech.ConfigLoader.getSparkSession
import org.apache.log4j.Logger

/**
 * @author Vengat
 *
 *         This app will process the unstructured
 */
object BikeWithComplexDataSets extends App {

  case class Bike(model: String, price: Double, city: String, kmsDriven: Double, owner: String, age: Double, power: Double, brand: String)

  @transient lazy val logger = Logger.getLogger(getClass.getName)

  val (spark, config) = getSparkSession(args)
  val readPath = config.getString("inputPath");
  val writePath = config.getString("outputPath")
  val datasetSchema = List(
    "bike_name"
    , "price"
    , "city"
    , "kms_driven"
    , "owner"
    , "age"
    , "power"
    , "brand"
  )

  val sparkContext = spark.sparkContext

  // Split the line and filter the valid lines
  def parseTextFile(lines: String): Array[String] = {
    lines.split('|').map(item => item.trim).filter(item => item.nonEmpty)
  }

  // Filter the Header row from source file
  def filterHeaderRow(row: String, datasetSchema: List[String]): Boolean = {
    val extractedRow = row.split('|').mkString("|")
    val schema = datasetSchema.mkString("|")
    extractedRow != schema
  }

  def mapActualSchema(row: String) = {
    val rowItems = row.split('|')
    Bike(rowItems(0), rowItems(1).toDouble, rowItems(2), rowItems(3).toDouble,
      rowItems(4), rowItems(5).toDouble, rowItems(6).toDouble, rowItems(7))
  }

  val sourceRDD = sparkContext.textFile(s"${readPath}complex_bike_datasets_op.txt")

  //  Parse the text file
  val parsedTextFileRDD = sourceRDD.map(parseTextFile).filter(line => line.length == 8).map(line => line.mkString("|"))
  // Filter the header from the actual data
  val filterHeaderRowRDD = parsedTextFileRDD.filter(row => filterHeaderRow(row, datasetSchema))
  val mappedSchemaRDD = filterHeaderRowRDD.map(mapActualSchema)

  val df = spark.createDataFrame(mappedSchemaRDD)
  df.show(false)
}