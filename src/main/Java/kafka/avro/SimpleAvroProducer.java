package kafka.avro;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * Inspired by:
 * https://github.com/aseigneurin/kafka-sandbox
 * http://aseigneurin.github.io/2016/03/04/kafka-spark-avro-producing-and-consuming-avro-messages.html
 *
 */
public class SimpleAvroProducer {

    public static void main(String[] args) throws InterruptedException, IOException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        String schemaFile = new String(Files.readAllBytes(Paths.get("./src/main/Java/kafka/avro/record.avsc")));

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(schemaFile);
        Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);

        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 1000; i++) {
            GenericData.Record avroRecord = new GenericData.Record(schema);
            avroRecord.put("str1", "Str 1-" + i);
            avroRecord.put("str2", "Str 2-" + i);
            avroRecord.put("int1", i);

            System.out.println("Sending record: " + avroRecord.toString());

            byte[] bytes = recordInjection.apply(avroRecord);

            ProducerRecord<String, byte[]> record = new ProducerRecord<>("avro-topic", bytes);
            producer.send(record);

            Thread.sleep(250);
        }

        producer.close();
    }
}