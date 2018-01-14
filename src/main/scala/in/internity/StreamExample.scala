package in.internity

import java.util.concurrent.TimeUnit

import com.lightbend.kafka.scala.streams.{KStreamS, StreamsBuilderS}
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.{JoinWindows, Produced}
import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods.parse
import org.json4s.native.Serialization.write

import scala.util.Try

/**
  * @author Shivansh <shiv4nsh@gmail.com>
  * @since 11/1/18
  */
trait StreamExample {
  implicit val formats: DefaultFormats.type = DefaultFormats
  val produced = Produced.`with`(Serdes.String(), Serdes.String())

  /**
    * This function consumes a topic and count the words in that stream
    *
    * @param intopic
    * @return
    */
  def wordCount(builder: StreamsBuilderS, intopic: String): KStreamS[String, String] = {
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
    *
    * @param intopic
    * @return
    */
  def readAndWriteJson(builder: StreamsBuilderS, intopic: String) = {
    val textLines: KStreamS[String, String] = builder.stream(intopic)
    textLines.mapValues(value => {
      val person = Try(parse(value).extract[Person]).toOption
      val personNameAndEmail = person.map(a => PersonNameAndEmail(a.name, a.email))
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
      (value1: String, value2: String) => s"""{"value2":$value2,"value1":$value1}""",
      JoinWindows.of(TimeUnit.MINUTES.toMillis(5)))
  }

}