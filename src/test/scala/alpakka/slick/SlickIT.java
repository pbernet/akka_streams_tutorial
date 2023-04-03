package alpakka.slick;

import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import scala.jdk.javaapi.FutureConverters;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * We use JUnit as test runner because of "type trouble"
 * when using PostgreSQLContainer with Scala
 * <br>
 * Doc:
 * https://www.testcontainers.org/modules/databases/postgres
 */
@Testcontainers
public class SlickIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(SlickIT.class);
    private static SlickRunner SLICK_RUNNER;
    private static String URL_WITH_MAPPED_PORT;

    @Container
    public static PostgreSQLContainer postgres = new PostgreSQLContainer(DockerImageName.parse("postgres:latest"))
        .withDatabaseName("test")
		.withUsername("test")
		.withPassword("test");

    @BeforeAll
    public static void setup() {
        String dbName = postgres.getDatabaseName();
        URL_WITH_MAPPED_PORT = postgres.getJdbcUrl();
        String username = postgres.getUsername();
        String pw = postgres.getPassword();
        LOGGER.info("DB: {} created at URL: {}, username: {}, password: {}", dbName, URL_WITH_MAPPED_PORT, username, pw);
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
        int noOfUsers = 20000;
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