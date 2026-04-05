package alpakka.firehose;

import alpakka.kinesis.FirehoseEcho;
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
import software.amazon.awssdk.services.firehose.FirehoseClient;
import software.amazon.awssdk.services.firehose.model.CreateDeliveryStreamRequest;
import software.amazon.awssdk.services.firehose.model.CreateDeliveryStreamResponse;
import software.amazon.awssdk.services.firehose.model.S3DestinationConfiguration;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Setup/run {@link alpakka.kinesis.FirehoseEcho} on ministack container
 * <p>
 * Doc:
 * https://github.com/nahuelnucera/ministack
 */
@Testcontainers
public class FirehoseEchoIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(FirehoseEchoIT.class);

    private static final String ACCESS_KEY = "test";
    private static final String SECRET_KEY = "test";
    private static final String REGION = "us-east-1";

    private static URI endpoint;

    @Container
    public static GenericContainer<?> ministack = new GenericContainer<>(DockerImageName.parse("nahuelnucera/ministack:latest"))
            .withExposedPorts(4566)
            .waitingFor(new HttpWaitStrategy()
                    .forPath("/_ministack/health")
                    .forPort(4566)
                    .withStartupTimeout(Duration.ofSeconds(60)));

    @BeforeAll
    public static void beforeAll() throws Exception {
        endpoint = URI.create("http://" + ministack.getHost() + ":" + ministack.getMappedPort(4566));
        LOGGER.info("MiniStack container started on endpoint: {}", endpoint);

        // Create S3 bucket via Java HttpClient (S3 SDK is not on the classpath)
        HttpClient httpClient = HttpClient.newHttpClient();
        HttpResponse<String> s3Response = httpClient.send(
                HttpRequest.newBuilder()
                        .uri(URI.create(endpoint + "/kinesis-activity-backup-local"))
                        .PUT(HttpRequest.BodyPublishers.noBody())
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        LOGGER.info("S3 bucket creation (status: {}): {}", s3Response.statusCode(), s3Response.body());

        // Create Firehose delivery stream with S3-only destination via SDK
        SdkHttpClient sdkHttpClient = ApacheHttpClient.builder().build();
        try (FirehoseClient firehose = FirehoseClient.builder()
                .endpointOverride(endpoint)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
                .region(Region.of(REGION))
                .httpClient(sdkHttpClient)
                .build()) {
            CreateDeliveryStreamResponse response = firehose.createDeliveryStream(CreateDeliveryStreamRequest.builder()
                    .deliveryStreamName("activity-to-elasticsearch-local")
                    .s3DestinationConfiguration(S3DestinationConfiguration.builder()
                            .roleARN("arn:aws:iam::000000000000:role/Firehose-Reader-Role")
                            .bucketARN("arn:aws:s3:::kinesis-activity-backup-local")
                            .build())
                    .build());
            LOGGER.info("Firehose delivery stream created: {}", response.deliveryStreamARN());
        }
    }

    @Test
    public void testLocal() {
        FirehoseEcho firehoseEcho = new FirehoseEcho(endpoint, ACCESS_KEY, SECRET_KEY, REGION);
        assertThat(firehoseEcho.run()).isEqualTo(10);
    }
}
