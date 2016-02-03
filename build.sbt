name := "scala-demos"

version := "1.0"

scalaVersion := "2.11.7"

val sparkVersion = "1.4.1"

libraryDependencies ++= Seq(
  // Spark and Spark Streaming
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,

  // Test dependencies
  "org.scalatest" %% "scalatest" % "2.2.4" % Test
)
