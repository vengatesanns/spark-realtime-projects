
name := "spark-realtime-projects"
organization := "com.hackprotech"
version := "1.0"
autoScalaLibrary := false
scalaVersion := "2.12.10"

val sparkVersion = "3.1.2"

val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "com.typesafe" % "config" % "1.4.1"
)

val testDependencies = Seq(
  "org.scalatest" %% "scalatest" % "3.2.7" % Test
)

libraryDependencies ++= sparkDependencies ++ testDependencies

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}



