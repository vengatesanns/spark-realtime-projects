package com.hackprotech

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object Employee extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkSession = SparkSession.builder().master("local[4]").appName("Employee").getOrCreate()


  val empDF = sparkSession.read.option("header", true).option("inferSchema", true).csv("src/main/resources/datasets/emp.csv")
  val empDF1 = empDF.repartition(col("emp_id"))
  val deptDF = sparkSession.read.option("header", true).option("inferSchema", true).option("delimiter", "|").csv("src/main/resources/datasets/dept.txt")

  empDF.show()
  deptDF.show()

  "".substring(0, 5)

  /*  empDF.createOrReplaceTempView("employee")
  deptDF.createOrReplaceTempView("dept")

  sparkSession.sql(
    """
      |select d.dept_id, d.dept_name, count(e.emp_id) from employee e
      |join dept d on d.dept_id = e.dept_id
      |group by d.dept_id, d.dept_name
      |""".stripMargin).show(false)


  sparkSession.sql(
    """
      |select * from employee
      |""".stripMargin).printSchema()

  sparkSession.sql(
    """
      |select * from dept
      |""".stripMargin).printSchema()


  //  val broadCastJoin = empDF.join(broadcast(deptDF), empDF("dept_id") === deptDF("dept_id"), "INNER")
  //  broadCastJoin.show(false)
  //
  //  val bdVariable = sparkSession.sparkContext.broadcast(deptDF)
  //
  //  val broadCastJoin = empDF.join(broadcast(deptDF), empDF("dept_id") === deptDF("dept_id"), "INNER")


  val columns = empDF.columns.map(column => col(column))
  empDF.select(columns: _*).show(2, false)*/

  val joinedDF = empDF.join(deptDF, empDF("dept_id") === deptDF("dept_id"), "leftsemi").repartition(3)

  joinedDF.explain()
  //  joinedDF.describe()

  joinedDF.show(false)
  joinedDF.coalesce(6).write.csv("target/test")

}
