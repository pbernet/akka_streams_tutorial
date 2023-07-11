package alpakka.s3;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Testcontainers;
import scala.jdk.javaapi.FutureConverters;

import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
public class S3EchoMinioIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3EchoMinioIT.class);

    // Credentials for client access
    private static final String ACCESS_KEY = "minio";
    private static final String SECRET_KEY = "minio123";

    @org.testcontainers.junit.jupiter.Container
    private static final MinioContainer minioContainer = new MinioContainer(
            new MinioContainer.CredentialsProvider(ACCESS_KEY, SECRET_KEY));

    @BeforeAll
    public static void beforeAll() {
        LOGGER.info("Started on host address: {}", minioContainer.getHostAddress());
    }

    @Test
    public void testLocal() throws InterruptedException, IOException {
        S3Echo echo = new S3Echo(minioContainer.getHostAddress(), ACCESS_KEY, SECRET_KEY);

        assertThat(FutureConverters.asJava(echo.run())).succeedsWithin(15, TimeUnit.SECONDS);
        LOGGER.info("Finished processing. Waiting with counting because of glitch...");
        Thread.sleep(1000);
        assertThat(Files.list(echo.localTmpDir()).count()).isEqualTo(10);
    }
}