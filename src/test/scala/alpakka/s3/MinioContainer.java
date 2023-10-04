package alpakka.s3;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;

import java.time.Duration;

// See also:
// https://github.com/akka/alpakka/blob/main/s3/src/test/scala/akka/stream/alpakka/s3/MinioContainer.scala
public class MinioContainer extends GenericContainer<MinioContainer> {

    private static final int DEFAULT_PORT = 9000;
    private static final String DEFAULT_IMAGE = "quay.io/minio/minio";
    private static final String DEFAULT_TAG = "latest";

    private static final String MINIO_ACCESS_KEY = "MINIO_ACCESS_KEY";
    private static final String MINIO_SECRET_KEY = "MINIO_SECRET_KEY";

    private static final String DEFAULT_STORAGE_DIRECTORY = "/data";
    private static final String HEALTH_ENDPOINT = "/minio/health/ready";

    public MinioContainer(CredentialsProvider credentials) {
        this(DEFAULT_IMAGE + ":" + DEFAULT_TAG, credentials);
    }

    public MinioContainer(String image, CredentialsProvider credentials) {
        super(image == null ? DEFAULT_IMAGE + ":" + DEFAULT_TAG : image);
        addExposedPort(DEFAULT_PORT);
        if (credentials != null) {
            withEnv(MINIO_ACCESS_KEY, credentials.getAccessKey());
            withEnv(MINIO_SECRET_KEY, credentials.getSecretKey());
        }
        withCommand("server", DEFAULT_STORAGE_DIRECTORY);
        setWaitStrategy(new HttpWaitStrategy()
                .forPort(DEFAULT_PORT)
                .forPath(HEALTH_ENDPOINT)
                .withStartupTimeout(Duration.ofMinutes(2)));
    }

    public String getHostAddress() {
        return getHost() + ":" + getMappedPort(DEFAULT_PORT);
    }


    public static class CredentialsProvider {
        private String accessKey;
        private String secretKey;

        public CredentialsProvider(String accessKey, String secretKey) {
            this.accessKey = accessKey;
            this.secretKey = secretKey;
        }

        public String getAccessKey() {
            return accessKey;
        }

        public String getSecretKey() {
            return secretKey;
        }
    }
}