name := "json_reader_evstafyev"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.5" % "provided"

//https://mvnrepository.com/artifact/org.json4s/json4s-jackson
//libraryDependencies += "org.json4s" % "json4s-jackson_2.10" % "3.6.7"

libraryDependencies += "com.lihaoyi" %% "pprint" % "0.5.6"

libraryDependencies ++= {

  lazy val circeVersion = "0.11.1"
  lazy val circeConfigVersion = "0.6.1"
  lazy val circeGenericVersion = "0.11.1"
  lazy val enumeratumCirceVersion = "1.5.22"

  Seq(
    "io.circe"              %% "circe-core"             % circeVersion,
    "io.circe"              %% "circe-generic"          % circeVersion,
    "io.circe"              %% "circe-config"           % circeConfigVersion,
    "io.circe"              %% "circe-generic-extras"   % circeGenericVersion,
    "com.beachape"          %% "enumeratum-circe"       % enumeratumCirceVersion,
//    "org.slf4j"             % "slf4j-api"               % "1.7.5",
//    "ch.qos.logback"        % "logback-classic"         % "1.0.9"
  )

}

resolvers ++= Seq(
  "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/"
)