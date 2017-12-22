package kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;



/**
 * Inspired by:
 * https://kafka.apache.org/documentation/streams
 *
 * using KTable see:
 * https://docs.confluent.io/current/streams/concepts.html#ktable
 *
 */
public class WordCountKStreams {

    public static void main(String[] args) {

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        //What to do when there is no initial offset in Kafka or if the current offset does not exist any more on the server (e.g. because that data has been deleted)
        //earliest: automatically reset the offset to the earliest offset
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> textLines = builder.stream("wordcount-input");

        KTable<String, Long> wordCount = textLines
                .flatMapValues(textLine -> Arrays.asList(textLine.toLowerCase().split("\\W+")))
                .filter((key, value) -> (!(value.equals("truth")))) //we don't want that do we?
                .filter((key, value) -> (!(value.equals(""))))
                .peek((key, value) -> System.out.println("Processing WORD count with value: " + value))
                .groupBy((key, word) -> word)
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("count-word-store"));
        wordCount.toStream().to("wordcount-output", Produced.with(Serdes.String(), Serdes.Long()));

        KTable<String, Long> messageCount = textLines
                .filter((key, value) -> ((value.contains("fakeNews"))))
                .peek((key, value) -> System.out.println("Processing MESSAGE count with value: " + value))
                .groupBy((key, message) -> message)
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("count-message-store"));
        messageCount.toStream().to("messagecount-output", Produced.with(Serdes.String(), Serdes.Long()));

        final KafkaStreams streams = new KafkaStreams(builder.build(), config);
        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                System.out.println("Got control-c cmd from shell, about to close stream...");
                Boolean shutdownResult = streams.close(10L, TimeUnit.SECONDS);
                if (shutdownResult) {
                    System.out.println("Stream closed successfully");
                } else {
                    System.out.println("Unable to close stream within the 10 seconds timeout");
                }
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.out.println("Exception occurred during startup: " + e.getMessage());
            System.exit(1);
        }

        System.exit(0);
    }
}