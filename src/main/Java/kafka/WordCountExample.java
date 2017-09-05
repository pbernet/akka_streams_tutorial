package kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * Inspired by:
 * https://github.com/gwenshap/kafka-streams-wordcount
 *
 *
 */
public class WordCountExample {

    public static void main(String[] args) throws Exception{

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        // Note: To re-run the demo, you need to use the offset reset tool:
        // https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KStreamBuilder builder = new KStreamBuilder();

        KStream<String, String> source = builder.stream("wordcount-input");

        final Pattern pattern = Pattern.compile("\\W+");
        KStream counts  = source.flatMapValues(value-> Arrays.asList(pattern.split(value.toLowerCase())))
                .map((key, value) -> new KeyValue<Object, Object>(value, value))
                .peek((key, value) -> System.out.println("Working on value: " + value))
                .filter((key, value) -> (!value.equals("truth")))
                .groupByKey()
                .count("CountStore").mapValues(value->Long.toString(value)).toStream();
        counts.to("wordcount-output");

        //TODO Add KTable input? What is the benefit?
        //https://github.com/apache/kafka/blob/0.11.0/streams/examples/src/main/java/org/apache/kafka/streams/examples/wordcount/WordCountDemo.java


        KafkaStreams streams = new KafkaStreams(builder, props);

        // TODO Research: This is for reset to work. Don't use in production - it causes the app to re-load the state from Kafka on every start
        streams.cleanUp();

        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                System.out.println("Shutdown Hook is running - about to close stream!");
                streams.close();
                System.out.println("Shutdown Hook is running - stream closed!");
            }
        });
        System.out.println("Application running for a loooong time...");
        Thread.sleep(500000000000000L);
    }
}