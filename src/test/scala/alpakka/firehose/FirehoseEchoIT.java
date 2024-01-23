package alpakka.firehose;

import alpakka.kinesis.FirehoseEcho;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.*;

/**
 * Setup/run {@link alpakka.kinesis.FirehoseEcho} on localStack container
 * <p>
 * Doc:
 * https://docs.localstack.cloud/user-guide/aws/kinesis-firehose
 * https://testcontainers.com/modules/localstack
 */
@Testcontainers
public class FirehoseEchoIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(FirehoseEchoIT.class);
    private static final int LOCALSTACK_PORT = 4566;

    @Container
    public static LocalStackContainer localStack = new LocalStackContainer(DockerImageName.parse("localstack/localstack:2.2"))
            // TODO Localstack v3 has "strict service loading", see:
            // https://github.com/localstack/localstack/releases/tag/v3.0.0
            // However, ELASTICSEARCH is not yet available via Testcontainers
            // see: org.testcontainers.containers.localstack.Service
            // Add ELASTICSEARCH once it is accessible via Testcontainers
            .withServices(FIREHOSE, S3, KINESIS)
            // Make sure that init_firehose.sh is executable and has linux line separator (LF)
            .withCopyFileToContainer(MountableFile.forClasspathResource("/localstack/init_firehose.sh", 700), "/etc/localstack/init/ready.d/init_firehose.sh")
            // When Elasticsearch is ready it spits out this line in the log. Takes up to 240 seconds on a 2012 vintage MacBook Pro...
            .waitingFor(Wait.forLogMessage(".*Active license is now \\[BASIC\\]; Security is disabled.*", 1).withStartupTimeout(Duration.ofSeconds(240)));

    @BeforeAll
    public static void beforeAll() throws InterruptedException, IOException {
        LOGGER.info("LocalStack container started on host address: {}", localStack.getEndpoint());

        ExecResult result = localStack.execInContainer("awslocal", "firehose", "list-delivery-streams");
        LOGGER.debug("Result exit code: {}", result.getExitCode());
        LOGGER.info("Check streams on container: {}", result.getStdout());

        ExecResult results3 = localStack.execInContainer("awslocal", "s3", "ls");
        LOGGER.debug("Result exit code: {}", results3.getExitCode());
        LOGGER.info("Check buckets on container: {}", results3.getStdout());
    }

    private static void browserClient() throws IOException {
        String os = System.getProperty("os.name").toLowerCase();
        String elasticsearchEndpoint = String.format("http://es-local.us-east-1.es.localhost.localstack.cloud:%s/_search", localStack.getMappedPort(LOCALSTACK_PORT));
        if (os.equals("mac os x")) {
            Runtime.getRuntime().exec("open " + elasticsearchEndpoint);
        } else if (os.equals("windows 10")) {
            Runtime.getRuntime().exec(String.format("cmd /c start %s", elasticsearchEndpoint));
        } else {
            LOGGER.info("Please open a browser at: {}", elasticsearchEndpoint);
        }
    }

    @Test
    public void testLocal() throws InterruptedException, IOException {
        FirehoseEcho firehoseEcho = new FirehoseEcho(localStack.getEndpointOverride(FIREHOSE), localStack.getAccessKey(), localStack.getSecretKey(), localStack.getRegion());
        assertThat(firehoseEcho.run()).isEqualTo(10);

        // Comment out to manually check Elasticsearch entries
        //browserClient();
        //Thread.sleep(1500000);
    }
}
