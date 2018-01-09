package in.internity

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.lightbend.kafka.scala.streams.{KStreamS, StreamsBuilderS}
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.{StreamsConfig, _}
import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods.parse
import org.json4s.native.Serialization.write

import scala.util.Try

/**
  * @author Shivansh <shiv4nsh@gmail.com>
  * @since 8/1/18
  */
object Boot extends App {
  implicit val formats: DefaultFormats.type = DefaultFormats
  val config: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass)
    p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass)
    p
  }

  val builder = new StreamsBuilderS()

  private def wordCount(intopic: String, outTopic: String) = {
    val produced = Produced.`with`(Serdes.String(), Serdes.String())
    val textLines: KStreamS[String, String] = builder.stream(intopic)
    val data: KStreamS[String, String] =
      textLines
        .flatMapValues(value => value.toLowerCase.split("\\W+").toIterable)
        .map((a,b) => (b,b))
        .groupByKey()
        .count("CountStore").toStream.mapValues(a => a.toString)
    data.to(outTopic, produced)
  }

  private def readAndWriteJson(intopic: String, outTopic: String) = {
    val produced = Produced.`with`(Serdes.String(), Serdes.String())
    val textLines: KStreamS[String, String] = builder.stream(intopic)
    val data: KStreamS[String, String] = textLines.mapValues(value => {
      val person = Try(parse(value).extract[Person]).toOption
      println("1::", person)
      val personNameAndEmail = person.map(a => PersonNameAndEmail(a.name, a.email))
      println("2::", personNameAndEmail)
      write(personNameAndEmail)
    })
    data.to(outTopic, produced)
  }


  wordCount("lines", "wordCount")
  readAndWriteJson("person", "personName")
  val streams: KafkaStreams = new KafkaStreams(builder.build(), config)
  streams.start()
  streams

  Runtime.getRuntime.addShutdownHook(new Thread(() => {
    streams.close(10, TimeUnit.SECONDS)
  }))
}

case class Person(name: String, age: Int, email: String)

case class PersonNameAndEmail(name: String, email: String)