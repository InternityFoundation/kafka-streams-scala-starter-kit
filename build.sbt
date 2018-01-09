name := "scala-kafka-streams-starter-kit"

version := "0.1"

scalaVersion := "2.12.4"

organization := "in.internity"

val kafkaStreamsScalaVersion = "0.1.0"

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.3.1",
  "org.json4s" %% "json4s-native" % "3.5.3",
  "com.lightbend" %% "kafka-streams-scala" % kafkaStreamsScalaVersion,
)
        