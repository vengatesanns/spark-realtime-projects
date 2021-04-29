name := "spark-course"
organization := "com.hackprotech"
version := "1.0"
autoScalaLibrary := false
scalaVersion := "2.12.10"

val sparkVersion = "3.1.1"

val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)

val testDependencies = Seq(
  "org.scalatest" %% "scalatest" % "3.2.7" % Test
)

libraryDependencies ++= sparkDependencies ++ testDependencies


