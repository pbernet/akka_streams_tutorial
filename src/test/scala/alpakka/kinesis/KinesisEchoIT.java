package alpakka.kinesis;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.localstack.LocalStackContainer;
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

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.KINESIS;

/**
 * This test needs some processing time for the stream setup to complete in the localStack container
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

    @Container
    public static LocalStackContainer localStack = new LocalStackContainer(DockerImageName.parse("localstack/localstack"))
            .withServices(KINESIS)
            // Make sure that init_kinesis.sh is executable and has linux line separator (LF)
            .withClasspathResourceMapping("/localstack/init_kinesis.sh", "/etc/localstack/init/ready.d/init_kinesis.sh", BindMode.READ_ONLY);
    // Also works but is deprecated; The suffix :1 is the shard count
    // see: https://docs.localstack.cloud/user-guide/aws/kinesis
    //.withEnv("KINESIS_INITIALIZE_STREAMS", STREAM_NAME + ":1");

    @BeforeAll
    public static void beforeAll() throws InterruptedException, IOException {
        LOGGER.info("LocalStack container started on host address: {}", localStack.getEndpoint());
        LOGGER.info("Waiting 10 seconds for stream setup to complete...");
        Thread.sleep(10000);

        ExecResult result = localStack.execInContainer("awslocal", "kinesis", "list-streams");
        LOGGER.debug("Result exit code: {}", result.getExitCode());
        LOGGER.info("Check streams on container: {}", result.getStdout());

        SdkHttpClient httpClient = ApacheHttpClient.builder().maxConnections(10).build();
        //SdkHttpClient httpClientLightweight = UrlConnectionHttpClient.builder().build();

        KinesisClient kinesisClient = KinesisClient
                .builder()
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(localStack.getAccessKey(), localStack.getSecretKey())))
                .region(Region.of(localStack.getRegion()))
                .httpClient(httpClient)
                .endpointOverride(localStack.getEndpoint())
                .build();

        createStream(kinesisClient);
        checkStreams(kinesisClient);
    }

    private static void createStream(KinesisClient kinesisClient) {
        CreateStreamRequest createStreamRequest = CreateStreamRequest
                .builder()
                // TODO Correctly detects existing stream, but we can't yet create a new stream with unique name
                .streamName(STREAM_NAME)
                //.streamName("clientCreatedUniqueNameDoesNotWork")
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

    @Test
    public void testLocal() {
        KinesisEcho kinesisEcho = new KinesisEcho(localStack.getEndpointOverride(KINESIS), localStack.getAccessKey(), localStack.getSecretKey(), localStack.getRegion());
        Integer result = kinesisEcho.run();
        assertThat(result).isEqualTo(10);
    }
}
