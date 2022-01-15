package alpakka.slick;

import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static scala.compat.java8.FutureConverters.globalExecutionContext;

/**
 * We use JUnit as test runner because of "type trouble"
 * when using PostgreSQLContainer with Scala
 *
 * Doc:
 * https://www.testcontainers.org/modules/databases/postgres
 *
 */
public class SlickIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(SlickIT.class);
    private static SlickRunner SLICK_RUNNER;
    private static String URL_WITH_MAPPED_PORT;

    @ClassRule
    public static PostgreSQLContainer postgres = new PostgreSQLContainer(DockerImageName.parse("postgres:latest"))
        .withDatabaseName("test")
		.withUsername("test")
		.withPassword("test");

    @BeforeClass
    public static void setup() {
        String dbName = postgres.getDatabaseName();
        URL_WITH_MAPPED_PORT = postgres.getJdbcUrl();
        String username = postgres.getUsername();
        String pw = postgres.getPassword();
        LOGGER.info("DB: {} created at URL: {}, username: {}, password: {}", dbName, URL_WITH_MAPPED_PORT, username, pw);
    }


    @AfterClass
    public static void teardown() {
        SLICK_RUNNER.terminate();
    }

    @Before
    public void before() {
        SLICK_RUNNER = SlickRunner.apply(URL_WITH_MAPPED_PORT);
        SLICK_RUNNER.createTableOnSession();
    }

    @After
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


    /**
     * We need the lib:
     * https://github.com/scala/scala-java8-compat
     * to provide interop between Scala Futures and Java 8 lambdas
     *
     */
    @Test
    public void populateAndReadUsersPaged() throws InterruptedException {
        int noOfUsers = 20000;
        SLICK_RUNNER.populateSync(noOfUsers);
        SLICK_RUNNER.processUsersPaged().onComplete(
                each -> assertThat(SLICK_RUNNER.counter().get()).isEqualTo(noOfUsers),
         globalExecutionContext());

        // Delay DB destroy and thus give processUsersPaged() time to complete
        Thread.sleep(10000);
    }

    @Test
    public void populateAndCountUsers() {
        int noOfUsers = 100;
        SLICK_RUNNER.populateSync(noOfUsers);
        assertThat(SLICK_RUNNER.getTotal()).isEqualTo(noOfUsers);
    }
}