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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * The whole world ingests data with Telegraf:
 * https://www.influxdata.com/time-series-platform/telegraf
 * <p>
 * We try to do it via the Java API:
 * https://github.com/influxdata/influxdb-client-java/tree/master/client-scala
 * <p>
 * We use the sync API, see discussion about write performance:
 * https://community.influxdata.com/t/influxdb-v2-java-client-write-performance/18919
 * <p>
 * There is also an async API:
 * https://github.com/influxdata/influxdb-client-java/tree/master/client#asynchronous-non-blocking-api
 * <p>
 * Doc line protocol:
 * https://docs.influxdata.com/influxdb/cloud/reference/syntax/line-protocol
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

    public CompletionStage<Done> writeTestPoints(int nPoints, String sensorID) {
        List<Integer> range = IntStream.rangeClosed(1, nPoints).boxed().collect(Collectors.toList());
        Source<Integer, NotUsed> source = Source.from(range);
        return source
                .groupedWithin(10, Duration.ofMillis(100))
                .mapAsyncUnordered(10, each -> this.eventHandlerPointBatch(each, influxDBClient.getWriteApiBlocking(), nPoints, sensorID))
                .runWith(Sink.ignore(), system);
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

        // Ref Doc: https://docs.influxdata.com/influxdb/v2.1/reference/syntax/line-protocol
        Path file = Paths.get("src/main/resources/line_protocol_data.txt");

        CompletionStage<Done> done = FileIO.fromPath(file)
                .via(Framing.delimiter(ByteString.fromString(System.lineSeparator()), 1024, FramingTruncation.ALLOW))
                .map(ByteString::utf8String)
                .grouped(2)
                .mapAsync(1, each -> this.eventHandlerRecordBatch(new ArrayList<>(each), writeApi))
                .runWith(Sink.ignore(), system);
        done.toCompletableFuture().get();
    }


    private CompletionStage<Done> eventHandlerRecordBatch(ArrayList<String> batch, WriteApiBlocking writeApi) {
        LOGGER.info("Writing batch with size: {}", batch.size());
        writeApi.writeRecords(WritePrecision.MS, batch);
        LOGGER.info("Successfully written batch with size: {}", batch.size());
        return CompletableFuture.completedFuture(Done.done());
    }

    private CompletionStage<Done> eventHandlerPoint(int hr, WriteApiBlocking writeApi, int nPoints, String sensorID) {
        LOGGER.debug("Writing point: {}-{} ", sensorID, hr);
        long testTime = System.nanoTime();
        Point point = createPoint(nPoints, sensorID, testTime, hr);
        writeApi.writePoint(point);
        return CompletableFuture.completedFuture(Done.done());
    }

    private CompletionStage<Done> eventHandlerPointBatch(List<Integer> hrs, WriteApiBlocking writeApi, int nPoints, String sensorID) {
        LOGGER.info("Writing points: {}-{} ", sensorID, hrs);
        List<Point> points = hrs.stream().map(each -> createPoint(nPoints, sensorID, System.nanoTime(), each)).collect(Collectors.toList());
        writeApi.writePoints(points);
        return CompletableFuture.completedFuture(Done.done());
    }

    @NotNull
    private Point createPoint(long nPoints, String sensorID, long testTime, int hr) {
        // A point is uniquely identified by the measurement name, tag set, and timestamp
        // If you submit duplicates, the field set becomes the union of the old field set and the new field set,
        // where any conflicts favor the new field set
        return Point
                .measurement("testMem")
                .addTag("sensorID", sensorID)
                .addTag("testTime", String.valueOf(testTime))
                .addTag("nPoints", String.valueOf(nPoints))   // used to verify completeness
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
