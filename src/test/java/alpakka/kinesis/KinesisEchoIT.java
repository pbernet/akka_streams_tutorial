package alpakka.kinesis;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.*;

import java.net.URI;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Setup/run {@link alpakka.kinesis.KinesisEcho} on ministack container
 * Use the classic sync AWS KinesisClient to create/delete streams
 * <p>
 * Doc:
 * https://github.com/NahuelNu/ministack
 * https://docs.aws.amazon.com/kinesis/latest/dev/introduction.html
 */
@Testcontainers
public class KinesisEchoIT {
    // MiniStack does not support CBOR binary encoding used by the Kinesis SDK.
    // Must be set before any AWS SDK class is loaded.
    static {
        System.setProperty("aws.cborEnabled", "false");
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(KinesisEchoIT.class);

    private static final String STREAM_NAME = "kinesisDataStreamProvisioned";

    private static final String ACCESS_KEY = "test";
    private static final String SECRET_KEY = "test";
    private static final String REGION = "us-east-1";

    private static KinesisClient kinesisClient;
    private static URI endpoint;

    @Container
    public static GenericContainer<?> ministack = new GenericContainer<>(DockerImageName.parse("nahuelnucera/ministack:latest"))
            .withExposedPorts(4566)
            .waitingFor(new HttpWaitStrategy()
                    .forPath("/_ministack/health")
                    .forPort(4566)
                    .withStartupTimeout(Duration.ofSeconds(60)));

    @BeforeAll
    public static void beforeAll() {
        endpoint = URI.create("http://" + ministack.getHost() + ":" + ministack.getMappedPort(4566));
        LOGGER.info("MiniStack container started on endpoint: {}", endpoint);

        SdkHttpClient httpClient = ApacheHttpClient.builder().maxConnections(10).build();

        kinesisClient = KinesisClient
                .builder()
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
                .region(Region.of(REGION))
                .httpClient(httpClient)
                .endpointOverride(endpoint)
                .build();

        createMainStream(kinesisClient);
        createStream(kinesisClient);
        checkStreams(kinesisClient);
    }

    @AfterAll
    public static void afterAll() throws InterruptedException {
        deleteStream(kinesisClient);
    }

    private static void createMainStream(KinesisClient kinesisClient) {
        CreateStreamRequest createStreamRequest = CreateStreamRequest
                .builder()
                .streamName(STREAM_NAME)
                .shardCount(1)
                .build();

        try {
            CreateStreamResponse createStreamResponse = kinesisClient.createStream(createStreamRequest);
            LOGGER.info("createStreamResponse for main stream: {}", createStreamResponse.responseMetadata().toString());
        } catch (ResourceInUseException ex) {
            LOGGER.info("Stream: {} already exists. Proceed...", STREAM_NAME);
        }
    }

    // This creates the additional stream
    private static void createStream(KinesisClient kinesisClient) {
        CreateStreamRequest createStreamRequest = CreateStreamRequest
                .builder()
                .streamName("kinesisDataStreamProvisioned_CreatedByClientSDK")
                .shardCount(1)
                .build();

        try {
            CreateStreamResponse createStreamResponse = kinesisClient.createStream(createStreamRequest);
            LOGGER.info("createStreamResponse: {}", createStreamResponse.responseMetadata().toString());
        } catch (ResourceInUseException ex) {
            LOGGER.info("Stream: {} already exists. Proceed...", STREAM_NAME);
        }
    }

    private static void checkStreams(KinesisClient kinesisClient) {
        LOGGER.info("Check streams via SDK");
        if (kinesisClient.listStreams().streamNames().isEmpty()) {
            LOGGER.info("No Kinesis data stream(s) setup for region: {}", REGION);
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
        KinesisEcho kinesisEcho = new KinesisEcho(endpoint, ACCESS_KEY, SECRET_KEY, REGION);
        Integer result = kinesisEcho.run();
        assertThat(result).isEqualTo(10);
    }
}
