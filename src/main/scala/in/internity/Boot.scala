package in.internity

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.lightbend.kafka.scala.streams.{KStreamS, StreamsBuilderS}
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.{JoinWindows, Produced}
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
  val produced = Produced.`with`(Serdes.String(), Serdes.String())

  /**
    * This function consumes a topic and count the words in that stream
    * @param intopic
    * @return
    */
  private def wordCount(intopic: String) = {
    val textLines: KStreamS[String, String] = builder.stream(intopic)
    textLines
      .flatMapValues(value => value.toLowerCase.split("\\W+").toIterable)
      .map((a, b) => (b, b))
      .groupByKey()
      .count("CountStore").toStream.mapValues(a => a.toString)
  }

  /**
    * This function consumes a topic and makes convert the Json to Case class
    * and remould it into Some other type and again converts it into Json
    * @param intopic
    * @return
    */
   def readAndWriteJson(intopic: String) = {
    val textLines: KStreamS[String, String] = builder.stream(intopic)
    textLines.mapValues(value => {
      val person = Try(parse(value).extract[Person]).toOption
      println("1::", person)
      val personNameAndEmail = person.map(a => PersonNameAndEmail(a.name, a.email))
      println("2::", personNameAndEmail)
      write(personNameAndEmail)
    })
  }

  /**
    * This function joins two streams and gives the resultant joined stream
    *
    * @param left
    * @param right
    * @return
    */
  def joinTwoStreams(left: KStreamS[String, String], right: KStreamS[String, String]) = {
    left.join(right,
      (value1: String, value2: String) => s"""{"display":$value2,"click":$value1}""",
      JoinWindows.of(TimeUnit.MINUTES.toMillis(5)))
  }


  val stream1 = wordCount("lines")

  val newStream =stream1.branch((a, b)=>a.equals(""))

  stream1.to("wordCount", produced)
  val stream2: KStreamS[String, String] = readAndWriteJson("person")
  stream2.to("wordCount", produced)
  val joinedStream = joinTwoStreams(stream1, stream2)
  joinedStream.to("combined", produced)

  val streams: KafkaStreams = new KafkaStreams(builder.build(), config)
  streams.start()
  streams

  Runtime.getRuntime.addShutdownHook(new Thread(() => {
    streams.close(10, TimeUnit.SECONDS)
  }))
}

case class Person(name: String, age: Int, email: String)

case class PersonNameAndEmail(name: String, email: String)