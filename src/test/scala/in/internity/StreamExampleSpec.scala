package in.internity


import java.util.Properties

import com.lightbend.kafka.scala.streams.StreamsBuilderS
import com.madewithtea.mockedstreams.MockedStreams
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.common.serialization.{Serdes, StringSerializer}
import org.apache.kafka.streams.{StreamsBuilder, StreamsConfig}
import org.json4s.native.JsonMethods._
import org.json4s.native.Serialization.write
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Matchers, WordSpec}

/**
  * @author Shivansh <shiv4nsh@gmail.com>
  * @since 11/1/18
  */
class StreamExampleSpec extends WordSpec with Matchers with BeforeAndAfterEach with BeforeAndAfterAll with StreamExample {

  implicit val configs = EmbeddedKafkaConfig(kafkaPort = 9092, zooKeeperPort = 7001)

  implicit val keySerializer = new StringSerializer

  override def beforeAll() = {
    EmbeddedKafka.start()
  }

  override def afterAll(): Unit = {
    EmbeddedKafka.stop()
  }

  val kafkaConf: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-scala-streams-example")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9002")
    p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass)
    p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass)
    p
  }


  "StreamExample" must {
    "take words and produce Wordcount" in {
      val inputTopic = "word"
      val inputString = "Hi this is Shivansh and my twitter handle is @shiv4nsh"
      val res = MockedStreams().topology { builder: StreamsBuilder =>
        wordCount(new StreamSTest(builder), inputTopic).to("Output1")
      }.config(kafkaConf)
        .input[String, String](inputTopic, Serdes.String(), Serdes.String(), Seq(("", inputString)))
        .output[String, String]("Output1", Serdes.String(), Serdes.String(), 9)
      res.toList.size shouldBe inputString.split(" ").distinct.size
    }

    "take one type of Json and return other" in {
      val inputTopic = "person"
      val inputString = Person("Shivansh", 23, "shiv4nsh@gmail.com")
      val res = MockedStreams().topology { builder: StreamsBuilder =>
        readAndWriteJson(new StreamSTest(builder), inputTopic).to("Output2")
      }.config(kafkaConf)
        .input[String, String](inputTopic, Serdes.String(), Serdes.String(), Seq(("", write(inputString))))
        .output[String, String]("Output2", Serdes.String(), Serdes.String(), 1)
      parse(res.toList.head._2).extract[PersonNameAndEmail] shouldBe PersonNameAndEmail("Shivansh", "shiv4nsh@gmail.com")
    }

    //FixMe: Fix this test
    "join two streams" in {

      val inputTopic1 = "wordjoin"

      val inputTopic2 = "personjoin"

      val inputString = "Hi this is Shivansh and my twitter handle is @shiv4nsh"
      val inputPerson = Person("Shivansh", 23, "shiv4nsh@gmail.com")
      val res = MockedStreams().topology { builder: StreamsBuilder =>
        val stream1 = wordCount( new StreamSTest(builder), inputTopic1)
        joinTwoStreams(stream1, stream1).to("joinedstreams")
      }.config(kafkaConf)
        .input[String, String](inputTopic1, Serdes.String(), Serdes.String(), Seq(("", inputString)))
        .output[String, String]("joinedstreams", Serdes.String(), Serdes.String(), 10)
      val result = res.toList
      res.toList.size shouldBe 10
    }
  }
}

class StreamSTest(streamBuilder: StreamsBuilder) extends StreamsBuilderS {
  override val inner = streamBuilder
}
