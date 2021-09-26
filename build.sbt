name := "PageRank"

version := "0.1"

scalaVersion := "2.12.10"

val sparkVersion = "3.1.2"
val typeSafeVersion = "1.4.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "com.typesafe" % "config" % typeSafeVersion
)
