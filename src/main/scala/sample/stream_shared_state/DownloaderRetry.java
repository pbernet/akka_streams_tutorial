package sample.stream_shared_state;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hc.client5.http.HttpRequestRetryStrategy;
import org.apache.hc.client5.http.HttpResponseException;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.HttpRequest;
import org.apache.hc.core5.http.HttpResponse;
import org.apache.hc.core5.http.io.HttpClientResponseHandler;
import org.apache.hc.core5.http.protocol.HttpContext;
import org.apache.hc.core5.util.TimeValue;
import org.apache.hc.core5.util.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

/**
 * A sync http file downloader with internal retry
 */
public class DownloaderRetry {
    private static final Logger LOGGER = LoggerFactory.getLogger(DownloaderRetry.class);
    private static final int DELAY_TO_RETRY_SECONDS = 20;

    public static void main(String[] args) throws Exception {

        URI url = new URI("http://httpstat.us/503");
        //URI url = new URI("http://127.0.0.1:6001/download/30");

        Path resFile = new DownloaderRetry().download(0, url, Paths.get(System.getProperty("java.io.tmpdir")).resolve(Paths.get("test.zip")));
        System.out.print("Downloaded file: " + resFile.toFile().getName() + " with size: " + resFile.toFile().length() + " bytes");
    }

    public Path download(int traceID, URI url, Path destinationFile) {
        LOGGER.info("TRACE_ID: {} about to download...", traceID);
        RequestConfig timeoutsConfig = RequestConfig.custom()
                .setConnectTimeout(Timeout.of(DELAY_TO_RETRY_SECONDS, TimeUnit.SECONDS)).build();

        try (CloseableHttpClient httpClient = HttpClientBuilder.create()
                .setDefaultRequestConfig(timeoutsConfig)
                .setRetryStrategy(new CustomHttpRequestRetryStrategy())
                .build()) {
            Path localPath = httpClient.execute(new HttpGet(url), new HttpResponseHandler(destinationFile));
            LOGGER.info("TRACE_ID: {} successfully downloaded", traceID);
            return localPath;

        } catch (IOException e) {
            throw new RuntimeException(String.format(
                    "Cannot download file %s or save it to %s.", url, destinationFile), e);
        }
    }

    /**
     * Customize retries for different types of (temporary) network related issues, which
     * manifest in IOException and HTTP 503
     */
    private static class CustomHttpRequestRetryStrategy implements HttpRequestRetryStrategy {

        private final Logger logger = LoggerFactory.getLogger(this.getClass());
        private final int maxRetriesCount = 3;
        private final int maxDurationMinutes = 1;

        /**
         * Triggered in case of exception
         *
         * @param exception The cause
         * @param execCount Retry attempt sequence number
         * @param context   {@link HttpContext}
         * @return True if we want to retry request, false otherwise
         */
        @Override
        public boolean retryRequest(HttpRequest request, IOException exception, int execCount, HttpContext context) {
            Throwable rootCause = ExceptionUtils.getRootCause(exception);
            logger.warn("Request attempt failed, root cause: {}", rootCause.toString());

            if (execCount >= maxRetriesCount) {
                logger.warn("Request failed after {} retries in {} minute(s)",
                        execCount, maxDurationMinutes);
                return false;

            } else if (rootCause instanceof SocketTimeoutException) {
                return true;

            } else if (rootCause instanceof SocketException
                    || rootCause instanceof InterruptedIOException
                    || exception instanceof SSLException) {
                try {
                    Thread.sleep(DELAY_TO_RETRY_SECONDS * 1000);
                } catch (InterruptedException e) {
                    e.printStackTrace(); // do nothing
                }
                return true;
            } else
                return false;
        }

        @Override
        public boolean retryRequest(HttpResponse response, int execCount, HttpContext context) {
            int httpStatusCode = response.getCode();
            if (httpStatusCode != 503)
                return false; // retry only on HTTP 503

            if (execCount >= maxRetriesCount) {
                logger.warn("File downloading failed after {} retries in {} minute(s)",
                        execCount, maxDurationMinutes);
                return false;

            } else {
                logger.warn("File downloading attempt failed, HTTP status code: {}", httpStatusCode);
                return true;
            }
        }

        @Override
        public TimeValue getRetryInterval(org.apache.hc.core5.http.HttpResponse response, int execCount, HttpContext context) {
            return TimeValue.ofSeconds(DELAY_TO_RETRY_SECONDS);
        }
    }

    private static class HttpResponseHandler implements HttpClientResponseHandler<Path> {

        private final Path targetFile;

        private HttpResponseHandler(Path targetFile) {
            this.targetFile = targetFile;
        }

        @Override
        public Path handleResponse(ClassicHttpResponse response) throws IOException {
            int httpStatusCode = response.getCode();
            if (httpStatusCode >= 300)
                throw new HttpResponseException(httpStatusCode, "HTTP error during file download. Cause: " + httpStatusCode);

            FileUtils.copyInputStreamToFile(response.getEntity().getContent(), targetFile.toFile());
            return targetFile;
        }
    }
}
