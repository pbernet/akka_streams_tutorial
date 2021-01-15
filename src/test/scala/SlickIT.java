import alpakka.slick.SlickRunner;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * We use JUnit as test runner because of "type trouble"
 * using testcontainers.org with Scala
 *
 */
public class SlickIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(SlickIT.class);
    private static SlickRunner runner;

    @ClassRule
    public static PostgreSQLContainer postgres = new PostgreSQLContainer(DockerImageName.parse("postgres:latest"));

    @AfterClass
    public static void teardown() {
        runner.terminate();
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

    /**
     * We need an additional lib to provide interop between Scala Futures and Java 8 lambdas
     * https://github.com/scala/scala-java8-compat
     *
     * However, using onComplete produces WARN msg in the log
     *
     * @throws InterruptedException
     */
    @Test
    public void populateAndReadUsers() throws InterruptedException {
        int noOfUsers = 100;
        runner.populate(noOfUsers);
        // Wait for async populate to finish
        Thread.sleep(1000);
        assertThat(noOfUsers).isEqualTo(runner.read().size());
//      runner.populate(noOfUsers).onComplete(
//      each -> assertThat(noOfUsers).isEqualTo(runner.read().size()),
//      globalExecutionContext());
    }
}