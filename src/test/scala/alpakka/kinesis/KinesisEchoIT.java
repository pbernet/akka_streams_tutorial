package alpakka.kinesis;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.KINESIS;

/**
 * Note that this test needs some processing time on lokalstack,
 * hence the sleeps for the setup to complete and for the download result to appear
 */
@Testcontainers
public class KinesisEchoIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(KinesisEchoIT.class);

    @Container
    public static LocalStackContainer localstack = new LocalStackContainer(DockerImageName.parse("localstack/localstack"))
            .withServices(KINESIS)
            // This works but is deprecated; The suffix :1 is the shard count
            // see: https://docs.localstack.cloud/user-guide/aws/kinesis/
            .withEnv("KINESIS_INITIALIZE_STREAMS", "testDataStreamProvisioned" + ":1");

    @BeforeAll
    public static void beforeAll() throws InterruptedException {
        LOGGER.info("LocalStack container started on host address: {}", localstack.getEndpoint());
        LOGGER.info("Waiting 10 seconds for data stream setup to complete...");
        Thread.sleep(10000);

        // TODO Create the stream via code - does not work yet
//        KinesisAsyncClient kinesisClient = KinesisClientUtil.createKinesisAsyncClient(
//                KinesisAsyncClient
//                        .builder()
//                        .endpointOverride(localstack.getEndpointOverride(KINESIS))
//                        .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey())))
//                        .region(Region.of(localstack.getRegion())));
//        CompletableFuture<CreateStreamResponse> fut = kinesisClient.createStream(CreateStreamRequest.builder().streamName("testDataStreamProvisioned" + ":1").build());
//        fut.thenAccept(res -> LOGGER.info("Streams found: " + kinesisClient.listStreams()));
    }

    @Test
    public void testLocal() {
        KinesisEcho kinesisEcho = new KinesisEcho(localstack.getEndpointOverride(KINESIS), localstack.getAccessKey(), localstack.getSecretKey(), localstack.getRegion());
        Integer result = kinesisEcho.run();
        assertThat(result).isEqualTo(10);
    }
}
