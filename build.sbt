name := "scala-kafka-streams-starter-kit"

version := "0.1"

scalaVersion := "2.12.4"

organization := "in.internity"

val kafkaStreamsScalaVersion = "0.1.0"

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.3.1",
  "org.json4s" %% "json4s-native" % "3.5.3",
  "com.lightbend" %% "kafka-streams-scala" % kafkaStreamsScalaVersion,
  "com.madewithtea" %% "mockedstreams" % "1.5.0" % Test,
  "org.scalatest" %% "scalatest" % "3.0.4" % Test,
  "net.manub" %% "scalatest-embedded-kafka-streams" % "1.0.0" % Test
)
        