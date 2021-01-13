import alpakka.slick.SlickRunner;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static scala.compat.java8.FutureConverters.globalExecutionContext;

/**
 * We use JUnit as testrunner because of "type trouble"
 * using testcontainers.org with Scala
 *
 */
public class SlickIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(SlickIT.class);
    private static SlickRunner runner;

    @ClassRule
    public static PostgreSQLContainer postgres = new PostgreSQLContainer(DockerImageName.parse("postgres:latest"));

    @BeforeClass
    public static void setup() {
    }

    @AfterClass
    public static void teardown() {
        //TODO tear down gracefully
        //runner.close();
    }

    @Before
    public void before() {
        String urlWithMappedPort = postgres.getJdbcUrl();
        String username = postgres.getUsername();
        String pw = postgres.getPassword();
        LOGGER.info("DB created at URL: {}, username: {}, password: {}", urlWithMappedPort, username, pw);

        runner = new SlickRunner(urlWithMappedPort);
        runner.createTableOnSession();
    }

    @After
    public void after() {
    //TODO
    }

    /**
     * We need an additional lib to provide interop between Scala Futures and Java 8 lambdas
     * https://github.com/scala/scala-java8-compat
     *
     * @throws InterruptedException
     */
    @Test
    public void writeAndReadTestData() {
        int noOfUsers = 100;
        runner.populate(noOfUsers).onComplete(
                each -> assertThat(noOfUsers).isEqualTo(runner.read().size()),
                globalExecutionContext());
    }
}