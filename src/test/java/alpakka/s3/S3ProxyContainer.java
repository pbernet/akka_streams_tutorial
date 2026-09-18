package alpakka.s3;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;

import java.time.Duration;

// https://github.com/gaul/s3proxy
public class S3ProxyContainer extends GenericContainer<S3ProxyContainer> {

    private static final int DEFAULT_PORT = 80;
    private static final String DEFAULT_IMAGE = "andrewgaul/s3proxy";

    private static final String S3PROXY_AUTHORIZATION = "S3PROXY_AUTHORIZATION";
    private static final String S3PROXY_IDENTITY = "S3PROXY_IDENTITY";
    private static final String S3PROXY_CREDENTIAL = "S3PROXY_CREDENTIAL";
    private static final String JCLOUDS_PROVIDER = "JCLOUDS_PROVIDER";
    private static final String JCLOUDS_FILESYSTEM_BASEDIR = "JCLOUDS_FILESYSTEM_BASEDIR";

    private static final String DEFAULT_STORAGE_DIRECTORY = "/data";
    private static final String HEALTH_ENDPOINT = "/healthz";

    public S3ProxyContainer(CredentialsProvider credentials) {
        this(DEFAULT_IMAGE, credentials);
    }

    public S3ProxyContainer(String image, CredentialsProvider credentials) {
        super(image == null ? DEFAULT_IMAGE : image);
        addExposedPort(DEFAULT_PORT);
        withEnv(S3PROXY_AUTHORIZATION, "aws-v2-or-v4");
        withEnv(JCLOUDS_PROVIDER, "filesystem");
        withEnv(JCLOUDS_FILESYSTEM_BASEDIR, DEFAULT_STORAGE_DIRECTORY);
        if (credentials != null) {
            withEnv(S3PROXY_IDENTITY, credentials.getAccessKey());
            withEnv(S3PROXY_CREDENTIAL, credentials.getSecretKey());
        }
        setWaitStrategy(new HttpWaitStrategy()
                .forPort(DEFAULT_PORT)
                .forPath(HEALTH_ENDPOINT)
                .withStartupTimeout(Duration.ofMinutes(2)));
    }

    public String getHostAddress() {
        return getHost() + ":" + getMappedPort(DEFAULT_PORT);
    }


    public static class CredentialsProvider {
        private final String accessKey;
        private final String secretKey;

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
