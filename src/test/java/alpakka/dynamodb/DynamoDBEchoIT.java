package alpakka.dynamodb;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import scala.jdk.javaapi.FutureConverters;

import java.net.URI;
import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Setup/run {@link alpakka.dynamodb.DynamoDBEcho} on ministack container
 * <p>
 * Running this example against AWS:
 * Looks as if there is a way to delete a DB instance via the SDK:
 * https://docs.aws.amazon.com/code-library/latest/ug/rds_example_rds_DeleteDBInstance_section.html
 * However, getting the `dbInstanceIdentifier` via SDK is not straightforward
 * Therefore, we only run against ministack for now in order to avoid dangling resources
 * <p>
 * Doc:
 * https://pekko.apache.org/docs/pekko-connectors/current/dynamodb.html#aws-dynamodb
 */
@Testcontainers
public class DynamoDBEchoIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBEchoIT.class);

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
    public void testLocal() throws ExecutionException, InterruptedException {
        URI endpoint = URI.create("http://" + ministack.getHost() + ":" + ministack.getMappedPort(4566));
        DynamoDBEcho dynamoDBEcho = new DynamoDBEcho(endpoint, ACCESS_KEY, SECRET_KEY, REGION);
        int noOfItemsEven = 10;

        CompletionStage<Object> result = FutureConverters.asJava(dynamoDBEcho.run(noOfItemsEven));
        assertThat(result.toCompletableFuture().get()).isEqualTo(noOfItemsEven / 2);
    }
}
