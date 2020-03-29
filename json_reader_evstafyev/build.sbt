name := "json_reader_evstafyev"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.5"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
//libraryDependencies += "org.json4s" %% "json4s-jackson"