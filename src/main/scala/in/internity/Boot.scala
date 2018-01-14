package in.internity

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.lightbend.kafka.scala.streams.{KStreamS, StreamsBuilderS}
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{StreamsConfig, _}

/**
  * @author Shivansh <shiv4nsh@gmail.com>
  * @since 8/1/18
  */


object Boot extends App with StreamExample {

  val config: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass)
    p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass)
    p
  }

  val builder = new StreamsBuilderS()

  val stream1 = wordCount(builder, "lines")

  val newStream = stream1.branch((a, b) => a.equals(""))

  stream1.to("wordCount", produced)
  val stream2: KStreamS[String, String] = readAndWriteJson(builder, "person")
  stream2.to("personMinimal", produced)
  val joinedStream = joinTwoStreams(stream1, stream2)
  joinedStream.to("combined", produced)

  val streams: KafkaStreams = new KafkaStreams(builder.build(), config)
  streams.start()

  Runtime.getRuntime.addShutdownHook(new Thread(() => {
    streams.close(10, TimeUnit.SECONDS)
  }))
}

case class Person(name: String, age: Int, email: String)

case class PersonNameAndEmail(name: String, email: String)