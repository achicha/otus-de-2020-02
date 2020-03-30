// defaults
lazy val root = (project in file(".")).
  settings(
    name := "ClickStream",
    version := "0.1",
    scalaVersion := "2.11.12",
    mainClass in Compile := Some("ClickStream")
  )

// libraryDependencies
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.5" % "provided"

resolvers ++= Seq(
  "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/"
)

// JAR file settings
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
