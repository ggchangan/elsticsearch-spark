name := "study"

version := "1.0"

scalaVersion := "2.10.4"

fork := true

libraryDependencies ++= Seq(
  "org.elasticsearch" %% "elasticsearch-spark" % "2.3.2",
  "org.apache.spark"  %% "spark-core" % "1.6.1"
)