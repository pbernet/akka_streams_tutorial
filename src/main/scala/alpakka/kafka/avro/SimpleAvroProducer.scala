package alpakka.kafka.avro

import com.sksamuel.avro4s.*
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties

/**
  * Not pekko streams related
  *
  * Prerequisite:
  * Run [[alpakka.env.KafkaServerEmbedded]]
  */
object SimpleAvroProducer {
  def main(args: Array[String]): Unit = {
    new Application();
    ()
  }

  private class Application {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:29092")
    props.put("key.serializer", classOf[StringSerializer].getName)
    props.put("value.serializer", classOf[AvroSerializer].getName)

    val producer = new KafkaProducer[String, AvroRecord](props)

    try for (i <- 0 until 100) {
      val avroRecord = AvroRecord(s"Str 1-$i", s"Str 2-$i", i)
      println(s"Sending record: $avroRecord")

      val record = new ProducerRecord[String, AvroRecord]("avro-topic", avroRecord)
      producer.send(record)

      Thread.sleep(100)
    } finally producer.close()
  }
}

class AvroSerializer extends org.apache.kafka.common.serialization.Serializer[AvroRecord] {
  override def serialize(topic: String, data: AvroRecord): Array[Byte] = {
    val baos = new java.io.ByteArrayOutputStream()
    val avroOutputStream = AvroOutputStream.binary[AvroRecord].to(baos).build()
    avroOutputStream.write(data)
    avroOutputStream.close()
    baos.toByteArray
  }
}
