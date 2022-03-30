name := "spark-realtime-projects"
organization := "com.hackprotech"
version := "2.0.0"
autoScalaLibrary := true
scalaVersion := "2.13.8"

val sparkVersion = "3.2.0"

val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "com.typesafe" % "config" % "1.4.2"
)

val testDependencies = Seq(
  "org.scalatest" %% "scalatest" % "3.2.11" % Test
)

libraryDependencies ++= sparkDependencies ++ testDependencies

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}



