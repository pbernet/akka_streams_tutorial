package alpakka.kinesis;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.*;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.KINESIS;

/**
 * This test needs some processing time
 * for the stream setup to complete in the localStack container
 * Reading the stream info via SDK does not work as expected
 * <p>
 * Doc:
 * https://testcontainers.com/modules/localstack
 * https://docs.localstack.cloud/user-guide/aws/kinesis
 * https://stackoverflow.com/questions/76106522/automatically-create-a-kinesis-data-stream-in-localstack-as-part-of-docker-compo
 */
@Testcontainers
public class KinesisEchoIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(KinesisEchoIT.class);

    private static final String STREAM_NAME = "testDataStreamProvisioned";

    @Container
    public static LocalStackContainer localStack = new LocalStackContainer(DockerImageName.parse("localstack/localstack"))
            .withServices(KINESIS)
            // Works but is deprecated; The suffix :1 is the shard count
            // see: https://docs.localstack.cloud/user-guide/aws/kinesis
            .withEnv("KINESIS_INITIALIZE_STREAMS", STREAM_NAME + ":1");
    // Also works. Make sure that init.sh is executable
    //.withClasspathResourceMapping("/localstack", "/etc/localstack/init/ready.d/", BindMode.READ_ONLY);

    @BeforeAll
    public static void beforeAll() throws InterruptedException, IOException {
        LOGGER.info("LocalStack container started on host address: {}", localStack.getEndpoint());
        LOGGER.info("Waiting 10 seconds for stream setup to complete...");
        Thread.sleep(10000);

        ExecResult result = localStack.execInContainer("awslocal", "kinesis", "list-streams");
        LOGGER.debug("Result exit code: {}", result.getExitCode());
        LOGGER.info("Check streams on container: {}", result.getStdout());

        KinesisClient kinesisClient = KinesisClient
                .builder()
                // 1) Asks for an additional security token included in the request to createStream
                //.credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey())))
                // 2) Sets up the stream resource, with cred profile read from local config
                //.credentialsProvider(ProfileCredentialsProvider.create())
                .region(Region.of(localStack.getRegion()))
                .httpClient(UrlConnectionHttpClient.builder().build())
                .build();

        // Does not work with 1) NOR 2)
        //createStream(kinesisClient);

        // Seems to work without credentials, but...
        if (kinesisClient.listStreams().streamNames().isEmpty()) {
            LOGGER.info("Check streams via SDK. No Kinesis data streams setup for region: {}", localStack.getRegion());
        } else {
            kinesisClient.listStreams().streamNames().forEach(each -> {
                DescribeStreamSummaryRequest describeStreamSummaryRequest = DescribeStreamSummaryRequest.builder().streamName(STREAM_NAME).build();
                DescribeStreamSummaryResponse describeStreamSummaryResponse = kinesisClient.describeStreamSummary(describeStreamSummaryRequest);
                // Why do we see three stream instances with the same ARN?
                // Why is the accountId NOT 000000000000 (= the default localstack accountId)?
                LOGGER.info("Check streams via SDK. StreamSummaryResponse: " + describeStreamSummaryResponse.streamDescriptionSummary().streamARN());
            });
        }
    }

    private static void createStream(KinesisClient kinesisClient) {
        CreateStreamRequest createStreamRequest = CreateStreamRequest.builder().streamName(STREAM_NAME).shardCount(1).streamModeDetails(StreamModeDetails.builder().streamMode(StreamMode.PROVISIONED).build()).build();

        try {
            // The ARN stream name is scoped by the AWS account and the Region.
            // That is, two streams in two different accounts can have the same name,
            // and two streams in the same account, but in two different Regions, can have the same name.
            CreateStreamResponse createStreamResponse = kinesisClient.createStream(createStreamRequest);
            LOGGER.info("createStreamResponse: " + createStreamResponse.responseMetadata().toString());
        } catch (ResourceInUseException ex) {
            LOGGER.info("testDataStreamProvisioned already exists. Proceed...");
        }
    }

    @Test
    public void testLocal() {
        KinesisEcho kinesisEcho = new KinesisEcho(localStack.getEndpointOverride(KINESIS), localStack.getAccessKey(), localStack.getSecretKey(), localStack.getRegion());
        Integer result = kinesisEcho.run();
        assertThat(result).isEqualTo(10);
    }
}
