package tools;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hc.client5.http.HttpRequestRetryStrategy;
import org.apache.hc.core5.http.HttpRequest;
import org.apache.hc.core5.http.HttpResponse;
import org.apache.hc.core5.http.protocol.HttpContext;
import org.apache.hc.core5.util.TimeValue;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;

/**
 * Customize retries for different types of (temporary) network related issues,
 * which manifest in an IOException.
 * Also retry on server error (HTTP 500) and Gateway timeout (HTTP 503)
 */
class HttpRequestRetryStrategyOpenAI implements HttpRequestRetryStrategy {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpRequestRetryStrategyOpenAI.class);
    private final int maxRetriesCount = 5;
    private final int retryIntervalSeconds = 5;

    public static void main(String[] args) throws Exception {
        new OpenAICompletions().postRequest(new JSONObject(), "http://httpstat.us/500");
    }

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
        LOGGER.warn("Request attempt failed, root cause: {}", rootCause.toString());

        if (execCount >= maxRetriesCount) {
            LOGGER.warn("Request failed after: {} retries",
                    execCount);
            return false;

        } else if (rootCause instanceof SocketTimeoutException) {
            return true;

        } else if (rootCause instanceof SocketException
                || rootCause instanceof InterruptedIOException
                || exception instanceof SSLException) {
            try {
                Thread.sleep(retryIntervalSeconds * 1000);
            } catch (InterruptedException e) {
                LOGGER.error("This should not happen: ", e);
            }
            return true;
        } else
            return false;
    }

    @Override
    public boolean retryRequest(HttpResponse response, int execCount, HttpContext context) {
        int httpStatusCode = response.getCode();
        if (httpStatusCode == 500 || httpStatusCode == 503) {
            if (execCount >= maxRetriesCount) {
                LOGGER.warn("Request failed after: {} retries",
                        execCount);
                return false;
            } else {
                LOGGER.warn("Request attempt: {}/{} failed, HTTP status code: {}. Retry...", execCount, maxRetriesCount, httpStatusCode);
                return true;
            }
        } else return false;
    }

    @Override
    public TimeValue getRetryInterval(org.apache.hc.core5.http.HttpResponse response, int execCount, HttpContext context) {
        return TimeValue.ofSeconds(retryIntervalSeconds);
    }
}
