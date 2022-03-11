package alpakka.influxdb;

import akka.Done;
import akka.actor.ActorSystem;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import util.LogFileScanner;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Testcontainers
public class InfluxdbIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(InfluxdbIT.class);
    private static final Integer INFLUXDB_PORT = 8086;
    private static final ActorSystem actorSystem = ActorSystem.create("InfluxdbIT");

    public String searchAfterPattern;

    @org.testcontainers.junit.jupiter.Container
    public static GenericContainer influxDBContainer = new GenericContainer<>(DockerImageName.parse("influxdb"))
            .withExposedPorts(INFLUXDB_PORT);
    String influxURL = "http://localhost:" + influxDBContainer.getMappedPort(INFLUXDB_PORT);
    InfluxdbWriter influxDBWriter = new InfluxdbWriter(influxURL, "abcdefgh", "testorg", "testbucket", actorSystem);
    InfluxdbReader influxDBReader = new InfluxdbReader(influxURL, "abcdefgh", "testorg", "testbucket", actorSystem);

    @BeforeAll
    public static void setupBeforeClass() throws IOException, InterruptedException {
        // We use the new official docker image, which has the (now separate cli) installed
        // Doc: https://docs.influxdata.com/influxdb/v2.1/reference/release-notes/influxdb/
        LOGGER.info("InfluxDB container listening on port: {}. Running: {} ", influxDBContainer.getMappedPort(INFLUXDB_PORT), influxDBContainer.isRunning());
        Container.ExecResult result = influxDBContainer.execInContainer("influx", "setup", "-b", "testbucket", "-f", "-o", "testorg", "-t", "abcdefgh", "-u", "admin", "-p", "adminadmin");
        LOGGER.info("Result exit code: " + result.getExitCode());
        LOGGER.info("Result stdout: " + result.getStdout());
        browserClient();
    }

    @AfterAll
    public static void shutdownAfterClass() throws InterruptedException {
        LOGGER.info("Sleep to keep influxdb instance running...");
        Thread.sleep(10000000);
    }

    @BeforeEach
    public void setupBeforeTest(TestInfo testInfo) {
        searchAfterPattern = String.format("Starting test: %s", testInfo.getTestMethod().toString());
        LOGGER.info(searchAfterPattern);
    }

    @Test
    @Order(1)
    public void testWriteAndRead() {
        int maxClients = 5;
        int nPoints = 1000;

        List<CompletionStage<Done>> futList = IntStream.rangeClosed(1, maxClients).boxed().parallel()
                .map(i -> influxDBWriter.writeTestPoints(nPoints, "sensor" + i))
                .collect(Collectors.toList());
        assertThat(CompletableFuture.allOf(futList.toArray(new CompletableFuture[futList.size()]))).succeedsWithin(4 * maxClients, TimeUnit.SECONDS);

        assertThat(influxDBReader.getQuerySync("testMem").length()).isEqualTo(nPoints * maxClients);
        assertThat(influxDBReader.fluxQueryCount("testMem")).isEqualTo(nPoints * maxClients);
        assertThat(new LogFileScanner("logs/application.log").run(1, 2, searchAfterPattern, "ERROR").length()).isEqualTo(0);
    }

    @Test
    @Order(2)
    public void testWriteAndReadLineProtocol() throws ExecutionException, InterruptedException {
        int nPoints = 10;
        influxDBWriter.writeTestPointsFromLineProtocolSync();
        assertThat(influxDBReader.getQuerySync("testMemLP").length()).isEqualTo(nPoints);
    }

    @Test
    @Order(3)
    public void testWriteContinuously() throws ExecutionException, InterruptedException {
        influxDBReader.run();
        influxDBWriter.writeTestPointEverySecond("sensorPeriodic");
    }

    // login with admin/adminadmin
    private static void browserClient() throws IOException {
        String os = System.getProperty("os.name").toLowerCase();
        String influxURL = "http://localhost:" + influxDBContainer.getMappedPort(INFLUXDB_PORT);
        if (os.equals("mac os x")) {
            Runtime.getRuntime().exec("open " + influxURL);
        } else {
            LOGGER.info("Please open a browser at: {}", influxURL);
        }
    }
}
