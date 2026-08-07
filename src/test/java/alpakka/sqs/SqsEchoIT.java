package alpakka.sqs;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.net.URI;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Setup/run {@link alpakka.sqs.SqsEcho} on ministack container
 */
@Testcontainers
public class SqsEchoIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqsEchoIT.class);

    private static final String ACCESS_KEY = "test";
    private static final String SECRET_KEY = "test";
    private static final String REGION = "us-east-1";

    @Container
    public static GenericContainer<?> ministack = new GenericContainer<>(DockerImageName.parse("nahuelnucera/ministack:latest"))
            .withExposedPorts(4566)
            .waitingFor(new HttpWaitStrategy()
                    .forPath("/_ministack/health")
                    .forPort(4566)
                    .withStartupTimeout(Duration.ofSeconds(60)));

    @BeforeAll
    public static void beforeAll() {
        URI endpoint = URI.create("http://" + ministack.getHost() + ":" + ministack.getMappedPort(4566));
        LOGGER.info("MiniStack container started on endpoint: {}", endpoint);
    }

    @Test
    public void testLocal() {
        URI endpoint = URI.create("http://" + ministack.getHost() + ":" + ministack.getMappedPort(4566));
        SqsEcho sqsEcho = new SqsEcho(endpoint, ACCESS_KEY, SECRET_KEY, REGION);
        Integer result = sqsEcho.run();
        assertThat(result).isEqualTo(10);
    }
}
