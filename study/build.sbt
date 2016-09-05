name := "study"

version := "1.0"

scalaVersion := "2.10.4"

fork := true

libraryDependencies ++= Seq(
  "org.elasticsearch" %% "elasticsearch-spark" % "2.1.1",
  "org.apache.spark"  %% "spark-core" % "1.4.1",
  "com.datastax.spark" %% "spark-cassandra-connector" % "1.4.1",
  "log4j" % "log4j" % "1.2.17"
)