package alpakka.slick;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import org.junit.Rule;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import scala.jdk.javaapi.FutureConverters;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Use JUnit as test runner because of "type trouble"
 * when using PostgreSQLContainer with Scala
 * <br>
 * Use ToxiProxy (= a TCP proxy running in a docker container) to test non-happy-path
 * Doc:
 * https://www.testcontainers.org/modules/databases/postgres
 * https://www.testcontainers.org/modules/toxiproxy/
 */
@Testcontainers
public class SlickIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(SlickIT.class);
    private static SlickRunner SLICK_RUNNER;
    private static String URL_WITH_MAPPED_PORT;

    @Rule
    public static Network network = Network.newNetwork();

    @Container
    public static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(DockerImageName.parse("postgres:latest"))
            .withDatabaseName("test")
            .withUsername("test")
            .withPassword("test")
            .withNetwork(network)
            .withNetworkAliases("postgres");

    @Container
    public static ToxiproxyContainer toxiproxy = new ToxiproxyContainer("ghcr.io/shopify/toxiproxy:2.5.0")
            .withNetwork(network);

    @BeforeAll
    public static void setup() throws IOException {
        String dbName = postgres.getDatabaseName();

        final ToxiproxyClient toxiproxyClient = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());
        // ToxiProxyContainer reserves 31 ports, starting at 8666
        final Proxy proxy = toxiproxyClient.createProxy("postgres", "0.0.0.0:8666", "postgres:5432");

        // TODO Add more toxics
        proxy.toxics()
                .latency("latency", ToxicDirection.DOWNSTREAM, 3)
                .setJitter(2);

        // Wire via ToxiProxy
        URL_WITH_MAPPED_PORT = postgres.getJdbcUrl().replace(postgres.getMappedPort(5432).toString(), toxiproxy.getMappedPort(8666).toString());

        String username = postgres.getUsername();
        String pw = postgres.getPassword();
        LOGGER.info("DB: {} created, reached via ToxiProxy at URL: {}, username: {}, password: {}", dbName, URL_WITH_MAPPED_PORT, username, pw);
    }


    @AfterAll
    public static void teardown() {
        SLICK_RUNNER.terminate();
    }

    @BeforeEach
    public void before() {
        SLICK_RUNNER = SlickRunner.apply(URL_WITH_MAPPED_PORT);
        SLICK_RUNNER.createTableOnSession();
    }

    @AfterEach
    public void after() {
        SLICK_RUNNER.dropTableOnSession();
        SLICK_RUNNER.session().close();
    }


    @Test
    public void populateAndReadUsers() {
        int noOfUsers = 100;
        SLICK_RUNNER.populateSync(noOfUsers);
        assertThat(SLICK_RUNNER.readUsersSync().size()).isEqualTo(noOfUsers);
    }

    @Test
    public void populateAndReadUsersPaged() {
        int noOfUsers = 10000;
        SLICK_RUNNER.populateSync(noOfUsers);

        // Done via counter to avoid Scala->Java result collection conversion "type trouble"
        assertThat(FutureConverters.asJava(SLICK_RUNNER.processUsersPaged())).succeedsWithin(5, TimeUnit.SECONDS);
        assertThat(SLICK_RUNNER.counter().get()).isEqualTo(noOfUsers);
    }

    @Test
    public void populateAndCountUsers() {
        int noOfUsers = 100;
        SLICK_RUNNER.populateSync(noOfUsers);
        assertThat(SLICK_RUNNER.getTotal()).isEqualTo(noOfUsers);
    }
}