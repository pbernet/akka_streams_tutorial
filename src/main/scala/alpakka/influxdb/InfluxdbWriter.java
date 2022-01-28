package alpakka.influxdb;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.javadsl.*;
import akka.util.ByteString;
import com.influxdb.LogLevel;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Doc discussion write performances:
 * https://community.influxdata.com/t/influxdb-v2-java-client-write-performance/18919
 *
 * Doc line protocol:
 * https://docs.influxdata.com/influxdb/cloud/reference/syntax/line-protocol
 *
 */
public class InfluxdbWriter {
    private final InfluxDBClient influxDBClient;
    private static final Logger LOGGER = LoggerFactory.getLogger(InfluxdbWriter.class);

    String token;
    String baseURL;
    String org;
    String bucket;
    ActorSystem system;

    public InfluxdbWriter(String baseURL, String token, String org, String bucket, ActorSystem system) {
        this.token = token;
        this.baseURL = baseURL;
        this.org = org;
        this.bucket = bucket;
        this.system = system;
        this.influxDBClient = InfluxDBClientFactory.create(this.baseURL, this.token.toCharArray(), this.org, this.bucket).setLogLevel(LogLevel.BASIC);
    }

    public void writeTestPoints(int nPoints, String sensorID) throws ExecutionException, InterruptedException {
        WriteApiBlocking writeApi = influxDBClient.getWriteApiBlocking();

        List<Integer> range = IntStream.rangeClosed(1, nPoints).boxed().collect(Collectors.toList());
        Source<Integer, NotUsed> source = Source.from(range);
        CompletionStage<Done> done = source
                .throttle(1, Duration.ofMillis(100))
                .mapAsync(1, each -> this.eventHandlerPoint(each, writeApi, nPoints, sensorID))
                .runWith(Sink.ignore(), system);
        done.toCompletableFuture().get();
        LOGGER.info("Finished writing records for: {}", sensorID);
    }

    // TODO Do it with akka-streams
    public void writeTestPointsParallel(int nPoints, int parFactor) {
        String sensorIDBase = "sensor";


    }

    public void writeTestPointEverySecond(String sensorID) throws ExecutionException, InterruptedException {
        WriteApiBlocking writeApi = influxDBClient.getWriteApiBlocking();

        CompletionStage<Done> done = Source.tick(
                Duration.ofSeconds(1),
                Duration.ofSeconds(1),
                                "tick")
                .mapAsync(1, each -> this.eventHandlerPoint(getRandomNumber(1, 100), writeApi, 0, sensorID))
                .runWith(Sink.ignore(), system);
        done.toCompletableFuture().get();
        LOGGER.info("This should not happen: Finished writing records for: {}", sensorID);
    }

    public void writeTestPointsFromLineProtocolSync() throws ExecutionException, InterruptedException {

        WriteApiBlocking writeApi = influxDBClient.getWriteApiBlocking();
        Path file = Paths.get("src/main/resources/line_protocol_data.txt");

        CompletionStage<Done> done = FileIO.fromPath(file)
                .via(Framing.delimiter(ByteString.fromString("\n"), 1024, FramingTruncation.ALLOW))
                .map(ByteString::utf8String)
                .grouped(2)
                //.throttle(1, Duration.ofMillis(500))
                .mapAsync(1, each -> this.eventHandler(each, writeApi))
                .runWith(Sink.ignore(), system);
        done.toCompletableFuture().get();
    }


    private CompletionStage<Done> eventHandler(List<String> batch, WriteApiBlocking writeApi) throws InterruptedException {
        LOGGER.info("Writing batch: {} with size: {}", batch, batch.size());
        writeApi.writeRecords(WritePrecision.MS, batch);
        return CompletableFuture.completedFuture(Done.done());
    }

    private CompletionStage<Done> eventHandlerPoint(int hr, WriteApiBlocking writeApi, int nPoints, String sensorID) throws InterruptedException {
        LOGGER.info("Writing point: {} ", hr);
        long testTime = System.nanoTime();
        Point point = createPoint(nPoints, sensorID, testTime, hr);
        writeApi.writePoint(point);        return CompletableFuture.completedFuture(Done.done());
    }

    @NotNull
    private Point createPoint(long nPoints, String sensorID, long testTime, int hr) {
        return Point
                .measurement("testPacket")
                .addTag("sensorID", sensorID)
                .addTag("testTime", String.valueOf(testTime))
                .addTag("nPoints", String.valueOf(nPoints))   //used to verify completeness
                .addField("hr", hr)
                .time(Instant.now().toEpochMilli(), WritePrecision.MS);
    }

    private int getRandomNumber(int min, int max) {
        return (int) ((Math.random() * (max - min)) + min);
    }

    public void shutdown() {
        influxDBClient.close();
        system.terminate();
    }
}
