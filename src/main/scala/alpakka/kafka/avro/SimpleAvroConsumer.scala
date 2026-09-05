package alpakka.kafka.avro

import com.sksamuel.avro4s.*
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer

import java.time.Duration
import java.util.Properties
import scala.jdk.CollectionConverters.*

/**
  * Not pekko streams related
  *
  * Prerequisite:
  * Run [[alpakka.env.KafkaServerEmbedded]]
  * Run [[alpakka.kafka.avro.SimpleAvroProducer]]
  */
object SimpleAvroConsumer {
  def main(args: Array[String]): Unit = {
    new Application();
    ()
  }

  private class Application {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:29092")
    props.put("group.id", "mygroup")
    props.put("key.deserializer", classOf[StringDeserializer].getName)
    props.put("value.deserializer", classOf[AvroDeserializer].getName)

    val consumer = new KafkaConsumer[String, AvroRecord](props)
    consumer.subscribe(List("avro-topic").asJava)

    var running = true
    while (running) {
      val records = consumer.poll(Duration.ofMillis(100))
      for (record: ConsumerRecord[String, AvroRecord] <- records.asScala) {
        val avroRecord = record.value()
        println(s"Receiving record: str1=${avroRecord.str1}, str2=${avroRecord.str2}, int1=${avroRecord.int1}")
      }
    }
  }
}

class AvroDeserializer extends org.apache.kafka.common.serialization.Deserializer[AvroRecord] {
  override def deserialize(topic: String, data: Array[Byte]): AvroRecord = {
    val avroSchema = AvroSchema[AvroRecord]
    val avroInputStream = AvroInputStream.binary[AvroRecord].from(data).build(avroSchema)
    val result = avroInputStream.iterator.next()
    avroInputStream.close()
    result
  }
}
