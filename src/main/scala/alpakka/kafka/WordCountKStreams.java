package alpakka.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;


/**
 * Inspired by:
 * https://kafka.apache.org/documentation/streams
 * <p>
 * Using KTable:
 * https://docs.confluent.io/current/streams/concepts.html#ktable
 * <p>
 * Interactive query for the local state store:
 * https://docs.confluent.io/current/streams/developer-guide/interactive-queries.html
 */
public class WordCountKStreams {

    public static void main(String[] args) {

        String tmpStateDir = Paths.get(System.getProperty("java.io.tmpdir")).resolve("kafka-state-dir").toString();

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.STATE_DIR_CONFIG, tmpStateDir);

        //What to do when there is no initial offset in Kafka or if the current offset does not exist any more on the server (e.g. because that data has been deleted)
        //earliest: automatically reset the offset to the earliest offset
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> textLines = builder.stream("wordcount-input");

        KTable<String, Long> wordCount = textLines
                .flatMapValues(textLine -> Arrays.asList(textLine.toLowerCase().split("\\W+")))
                .filter((key, value) -> (!(value.equals("truth")))) //we don't want that do we?
                .filter((key, value) -> (!(value.equals(""))))
                //.peek((key, value) -> System.out.println("Processing WORD count key: " + key + " with value: " + value))
                .groupBy((key, word) -> word)
                .count(Materialized.as("count-word-store"));
        wordCount.toStream().to("wordcount-output", Produced.with(Serdes.String(), Serdes.Long()));

        KTable<String, Long> messageCount = textLines
                .filter((key, value) -> ((value.contains("fakeNews"))))
                //.peek((key, value) -> System.out.println("Processing MESSAGE count key: " + key + " with value: " + value))
                .map((key, value) -> new KeyValue<>("total", value))
                .groupByKey()
                .count(Materialized.as("count-message-store"));
        messageCount.toStream().to("messagecount-output", Produced.with(Serdes.String(), Serdes.Long()));

        final KafkaStreams app = new KafkaStreams(builder.build(), config);
        final CountDownLatch latch = new CountDownLatch(1);

        try {
            addShutdownHook(app, latch);
            app.start();
            interactiveQuery(app);
            latch.await();
        } catch (Throwable e) {
            System.out.println("Exception occurred: " + e.getMessage());
            System.exit(1);
        }
        System.exit(0);
    }

    private static void interactiveQuery(KafkaStreams app) {
        Thread thread = new Thread("fakenews-interactive-queries") {
            @Override
            public void run() {
                ReadOnlyKeyValueStore<String, Long> keyValueStoreWords = WordCountKStreams.waitUntilStoreIsQueryable("count-word-store", QueryableStoreTypes.keyValueStore(), app, "fakenews");
                ReadOnlyKeyValueStore<String, Long> keyValueStoreMessages = WordCountKStreams.waitUntilStoreIsQueryable("count-message-store", QueryableStoreTypes.keyValueStore(), app, "total");

                while (true) {
                    // When running against restarted KafkaServerEmbedded, because of in-memory we get a InvalidStateStoreException here
                    System.out.println("Query WORD count fakenews total: " + keyValueStoreWords.get("fakenews"));
                    System.out.println("Query MESSAGES count total: " + keyValueStoreMessages.get("total"));
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                }
            }
        };
        thread.start();
    }

    private static void addShutdownHook(KafkaStreams app, CountDownLatch latch) {
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                System.out.println("Got control-c cmd from shell, about to close stream...");

                Boolean shutdownResult = app.close(Duration.ofMillis(10000));
                // cleanUp() deletes the application's *local* state dir (= STATE_DIR_CONFIG)
                // On restart of Kafka this local state dir folder will be restored

                //When you have a Kafka installation: Use the app-reset-tool
                //https://www.confluent.io/blog/data-reprocessing-with-kafka-streams-resetting-a-streams-application/
                //https://docs.confluent.io/current/streams/developer-guide/app-reset-tool.html
                app.cleanUp();

                if (shutdownResult) {
                    System.out.println("Stream closed successfully");
                } else {
                    System.out.println("Unable to close stream within the 10 seconds timeout");
                }
                latch.countDown();
            }
        });
    }

    private static ReadOnlyKeyValueStore<String, Long> waitUntilStoreIsQueryable(final String storeName,
                                                                                 final QueryableStoreType<ReadOnlyKeyValueStore<String, Long>> queryableStoreType,
                                                                                 final KafkaStreams streams,
                                                                                 final String key) {
        while (true) {
            try {
                System.out.println("Start checking if local store: " + storeName + " is reachable");
                ReadOnlyKeyValueStore<String, Long> keyValueStore = streams.store(StoreQueryParameters.fromNameAndType(storeName, queryableStoreType));
                keyValueStore.get(key);
                return keyValueStore;

            } catch (InvalidStateStoreException ignored) {
                System.out.println("Local store: " + storeName + " not yet ready for querying - sleep");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    // ignore
                }
            }
        }
    }
}