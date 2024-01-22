package alpakka.kinesis;

import org.junit.jupiter.api.AfterAll;
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
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.*;

import java.io.IOException;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.KINESIS;

/**
 * Setup/run {@link alpakka.kinesis.KinesisEcho} on localStack container
 * Additionally use the classic sync AWS KinesisClient to create/delete streams
 * <p>
 * Doc:
 * https://testcontainers.com/modules/localstack
 * https://docs.localstack.cloud/user-guide/aws/kinesis
 * https://stackoverflow.com/questions/76106522/automatically-create-a-kinesis-data-stream-in-localstack-as-part-of-docker-compo
 */
@Testcontainers
public class KinesisEchoIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(KinesisEchoIT.class);

    private static final String STREAM_NAME = "kinesisDataStreamProvisioned";

    private static KinesisClient kinesisClient;

    @Container
    public static LocalStackContainer localStack = new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.0.2"))
            .withServices(KINESIS)
            // Make sure that init_kinesis.sh is executable and has linux line separator (LF)
            .withCopyFileToContainer(MountableFile.forClasspathResource("/localstack/init_kinesis.sh", 700), "/etc/localstack/init/ready.d/init_kinesis.sh")
    // Also works but is deprecated; The suffix :1 is the shard count
    // see: https://docs.localstack.cloud/user-guide/aws/kinesis
    //.withEnv("KINESIS_INITIALIZE_STREAMS", STREAM_NAME + ":1");
            .waitingFor(Wait.forLogMessage(".*Starting persist data loop.*", 1).withStartupTimeout(Duration.ofSeconds(30)));

    @BeforeAll
    public static void beforeAll() throws InterruptedException, IOException {
        LOGGER.info("LocalStack container started on host address: {}", localStack.getEndpoint());

        ExecResult result = localStack.execInContainer("awslocal", "kinesis", "list-streams");
        LOGGER.debug("Result exit code: {}", result.getExitCode());
        LOGGER.info("Check streams on container: {}", result.getStdout());

        SdkHttpClient httpClient = ApacheHttpClient.builder().maxConnections(10).build();
        //SdkHttpClient httpClientLightweight = UrlConnectionHttpClient.builder().build();

        kinesisClient = KinesisClient
                .builder()
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(localStack.getAccessKey(), localStack.getSecretKey())))
                .region(Region.of(localStack.getRegion()))
                .httpClient(httpClient)
                .endpointOverride(localStack.getEndpoint())
                .build();

        createStream(kinesisClient);
        checkStreams(kinesisClient);
    }

    @AfterAll
    public static void afterAll() throws InterruptedException, IOException {
        deleteStream(kinesisClient);

        ExecResult result = localStack.execInContainer("awslocal", "kinesis", "list-streams");
        LOGGER.debug("Result exit code: {}", result.getExitCode());
        LOGGER.info("Check streams on container: {}", result.getStdout());
    }

    // This creates the additional stream, it is visible on the localstack container (see afterAll)
    // However, the subsequent checkStreams() call does not show this, maybe due to caching in SDK?
    private static void createStream(KinesisClient kinesisClient) {
        CreateStreamRequest createStreamRequest = CreateStreamRequest
                .builder()
                .streamName("kinesisDataStreamProvisioned_CreatedByClientSDK")
                .shardCount(1)
                .streamModeDetails(StreamModeDetails
                        .builder()
                        .streamMode(StreamMode.PROVISIONED)
                        .build())
                .build();

        try {
            // The ARN stream name is scoped by the AWS account and the Region.
            // That is, two streams in two different accounts can have the same name,
            // and two streams in the same account, but in two different Regions, can have the same name.
            CreateStreamResponse createStreamResponse = kinesisClient.createStream(createStreamRequest);
            LOGGER.info("createStreamResponse: " + createStreamResponse.responseMetadata().toString());
        } catch (ResourceInUseException ex) {
            LOGGER.info("Stream: {} already exists. Proceed...", STREAM_NAME);
        }
    }

    private static void checkStreams(KinesisClient kinesisClient) {
        LOGGER.info("Check streams via SDK");
        if (kinesisClient.listStreams().streamNames().isEmpty()) {
            LOGGER.info("No Kinesis data stream(s) setup for region: {}", localStack.getRegion());
        } else {
            kinesisClient.listStreams().streamNames().forEach(each -> {
                DescribeStreamSummaryRequest describeStreamSummaryRequest = DescribeStreamSummaryRequest.builder().streamName(STREAM_NAME).build();
                DescribeStreamSummaryResponse describeStreamSummaryResponse = kinesisClient.describeStreamSummary(describeStreamSummaryRequest);
                LOGGER.info("StreamSummaryResponse: {}", describeStreamSummaryResponse.streamDescriptionSummary().streamARN());
            });
        }
    }

    private static void deleteStream(KinesisClient kinesisClient) throws InterruptedException {
        DeleteStreamRequest deleteStreamRequest = DeleteStreamRequest
                .builder()
                .streamName(STREAM_NAME)
                .build();

        kinesisClient.deleteStream(deleteStreamRequest);
        Thread.sleep(1000);
        LOGGER.info("Successfully deleted stream: {} via SDK", STREAM_NAME);
    }


    @Test
    public void testLocal() {
        KinesisEcho kinesisEcho = new KinesisEcho(localStack.getEndpointOverride(KINESIS), localStack.getAccessKey(), localStack.getSecretKey(), localStack.getRegion());
        Integer result = kinesisEcho.run();
        assertThat(result).isEqualTo(10);
    }
}
