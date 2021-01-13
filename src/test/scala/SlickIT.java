import alpakka.slick.SlickRunner;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

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

    @Test
    public void writeAndReadTestData() {
        int noOfUsers = 100;
        runner.populate(noOfUsers);
        //TODO return and assert
        runner.read();
    }
}