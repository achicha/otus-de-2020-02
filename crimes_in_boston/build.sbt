// defaults
lazy val root = (project in file(".")).
  settings(
    name := "crimes_in_boston",
    version := "0.1",
    scalaVersion := "2.11.12",
    mainClass in Compile := Some("CrimesGuide")
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
