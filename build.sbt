name := "scala-spark-reddit-example"

version := "0.1"

scalaVersion := "2.12.10"
val sparkVersion = "3.1.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.scalactic" %% "scalactic" % "3.2.9",
  "org.scalatest" %% "scalatest" % "3.2.9" % "test",
  "io.sensesecure" % "hadoop-xz" % "1.4"
)