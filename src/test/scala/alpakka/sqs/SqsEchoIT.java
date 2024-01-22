package alpakka.sqs;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.SQS;

/**
 * Setup/run {@link alpakka.sqs.SqsEcho} on localStack container
 */
@Testcontainers
public class SqsEchoIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqsEchoIT.class);

    @Container
    public static LocalStackContainer localStack = new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.0.2"))
            .withServices(SQS)
            // https://docs.localstack.cloud/user-guide/aws/sqs/#queue-urls
            .withEnv("SQS_ENDPOINT_STRATEGY", "domain")
            // Redundant to createQueue(), left to show that this is a way to init a queue
            .withCopyFileToContainer(MountableFile.forClasspathResource("/localstack/init_sqs.sh", 700), "/etc/localstack/init/ready.d/init_sqs.sh");

    @BeforeAll
    public static void beforeAll() throws InterruptedException, IOException {
        LOGGER.info("LocalStack container started on host address: {}", localStack.getEndpoint());
        LOGGER.info("Waiting 10 seconds for queue setup to complete...");
        Thread.sleep(10000);

        org.testcontainers.containers.Container.ExecResult result = localStack.execInContainer("awslocal", "sqs", "list-queues");
        LOGGER.debug("Result exit code: {}", result.getExitCode());
        LOGGER.info("Check queues on container: {}", result.getStdout());
    }

    @Test
    public void testLocal() {
        SqsEcho sqsEcho = new SqsEcho(localStack.getEndpointOverride(SQS), localStack.getAccessKey(), localStack.getSecretKey(), localStack.getRegion());
        Integer result = sqsEcho.run();
        assertThat(result).isEqualTo(10);
    }
}
