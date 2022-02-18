package alpakka.influxdb;

import akka.actor.ActorSystem;
import org.junit.*;
import org.junit.rules.TestName;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import util.LogFileScanner;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class InfluxdbIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(InfluxdbIT.class);
    private static final Integer INFLUXDB_PORT = 8086;
    private static final ActorSystem actorSystem = ActorSystem.create("InfluxdbIT");

    @Rule
    public TestName testName = new TestName();
    public String searchAfterPattern;

    @ClassRule
    public static GenericContainer influxDBContainer = new GenericContainer<>(DockerImageName.parse("influxdb"))
            .withExposedPorts(INFLUXDB_PORT);
    String influxURL = "http://localhost:" + influxDBContainer.getMappedPort(INFLUXDB_PORT);
    InfluxdbWriter influxDBWriter = new InfluxdbWriter(influxURL, "abcdefgh", "testorg", "testbucket", actorSystem);
    InfluxdbReader influxDBReader = new InfluxdbReader(influxURL, "abcdefgh", "testorg", "testbucket", actorSystem);

    @BeforeClass
    public static void setupBeforeClass() throws IOException, InterruptedException {
        // We use the new official docker image (which has the now separate cli installed)
        // Doc: https://docs.influxdata.com/influxdb/v2.1/reference/release-notes/influxdb/
        LOGGER.info("InfluxDB container listening on port: {}. Running: {} ", influxDBContainer.getMappedPort(INFLUXDB_PORT), influxDBContainer.isRunning());
        Container.ExecResult result = influxDBContainer.execInContainer("influx", "setup", "-b", "testbucket", "-f", "-o", "testorg", "-t", "abcdefgh", "-u", "admin", "-p", "adminadmin");
        LOGGER.info("Result exit code: " + result.getExitCode());
        LOGGER.info("Result stdout: " + result.getStdout());
        browserClient();
    }

    @AfterClass
    public static void shutdownAfterClass() throws InterruptedException {
        LOGGER.info("Sleep to keep influxdb instance running...");
        Thread.sleep(10000000);
    }

    @Before
    public void setupBeforeTest() {
        searchAfterPattern = String.format("Starting test: %s", testName.getMethodName());
        LOGGER.info(searchAfterPattern);
    }

    @Test
    public void testWriteAndRead() throws InterruptedException {
        int maxClients = 5;
        int nPoints = 1000;

        IntStream.rangeClosed(1, maxClients).boxed().parallel().forEach(i -> {
            Runnable r = () -> influxDBWriter.writeTestPoints(nPoints, "sensor" + i);
            r.run();
        });

        // TODO Instead of waiting, try to collect done responses from writeTestPoints and wait for all of them to finish
        // Similar to SlickIT>>populateAndReadUsersPaged
        Thread.sleep(2000 * maxClients);
        assertThat(influxDBReader.getQuerySync("testPacket").length()).isEqualTo(nPoints * maxClients);
        assertThat(influxDBReader.fluxQueryCount("testPacket")).isEqualTo(nPoints * maxClients);
        assertThat(new LogFileScanner("logs/application.log").run(1, 2, searchAfterPattern, "ERROR").length()).isEqualTo(0);
    }

    @Test
    public void testWriteAndReadLineProtocol() throws ExecutionException, InterruptedException {
        int nPoints = 10;
        influxDBWriter.writeTestPointsFromLineProtocolSync();
        assertThat(influxDBReader.getQuerySync("testPacketLP").length()).isEqualTo(nPoints);
    }
    
    @Test
    public void xWriteContinuously() throws ExecutionException, InterruptedException {
        influxDBWriter.writeTestPointEverySecond("testPacketPeriod");
    }

    private static void browserClient() throws IOException {
        String os = System.getProperty("os.name").toLowerCase();
        String influxURL = "http://localhost:" + influxDBContainer.getMappedPort(INFLUXDB_PORT);
        if (os.equals("mac os x")){
          Runtime.getRuntime().exec("open " + influxURL);
        }
        else {
          LOGGER.info("Please open a browser at: {}", influxURL);
        }
    }
}
