package sample.stream_shared_state;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.ServiceUnavailableRetryStrategy;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.protocol.HttpContext;
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

/**
 * A simple file downloader with retry
 * For local testing use dummy file server: alpakka.env.FileServer
 *
 * Exponential backoff could be added:
 * https://stackoverflow.com/questions/35391005/how-to-use-an-exponential-backoff-strategy-with-apache-httpclient
 */
public class DownloaderRetry {
	private static final int DELAY_TO_RETRY_SECONDS = 20;

	public static void main(String[] args) throws Exception {

		//URI url = new URI("http://httpstat.us/503");
		URI url = new URI("http://127.0.0.1:6001/download/30");
		//URI url = new URI("http://127.0.0.1:6001/downloadflaky/30");

		Path resFile = new DownloaderRetry().download(url, LocalFileCache.localFileCache().resolve(Paths.get("test.zip")));
		System.out.print("Downloaded file: " + resFile.toFile().getName() + " with size: " + resFile.toFile().length() + " bytes");
	}

	public Path download(URI url, Path destinationFile) {
		RequestConfig timeoutsConfig = RequestConfig.custom()

				// The time to establish the connection with the remote host [http.connection.timeout].
				// Responsible for java.net.SocketTimeoutException: connect timed out.
				.setConnectTimeout(DELAY_TO_RETRY_SECONDS * 1000)

				// The time to wait for a connection from the connection manager/pool [http.connection-manager.timeout].
				// Responsible for org.apache.http.conn.ConnectionPoolTimeoutException.
				.setConnectionRequestTimeout(DELAY_TO_RETRY_SECONDS * 1000)

				// The time waiting for data after the connection was established [http.socket.timeout]. The maximum time
				// of inactivity between two data packets. Responsible for java.net.SocketTimeoutException: Read timed out.
				.setSocketTimeout(DELAY_TO_RETRY_SECONDS * 1000).build();

		try (CloseableHttpClient httpClient = HttpClientBuilder.create()
				.setDefaultRequestConfig(timeoutsConfig)
				.setRetryHandler(new CustomHttpRequestRetryHandler())
				.setServiceUnavailableRetryStrategy(new CustomServiceUnavailableRetryStrategy())
				.build()) {
			return httpClient.execute(new HttpGet(url), new HttpResponseHandler(destinationFile));

		} catch (IOException e) {
			throw new RuntimeException(String.format(
					"Cannot download file %s or save it to %s", url, destinationFile), e);
		}
	}

	/**
	 * Custom HttpRequestRetryHandler implementation to customize retries for different IOException
	 */
	private class CustomHttpRequestRetryHandler implements HttpRequestRetryHandler {

		private final Logger logger             = LoggerFactory.getLogger(this.getClass());
		private final int    maxRetriesCount    = 3;
		private final int    maxDurationMinutes = 1;

		/**
		 * Triggered only in case of exception
		 *
		 * @param exception      The cause
		 * @param executionCount Retry attempt sequence number
		 * @param context        {@link HttpContext}
		 * @return True if we want to retry request, false otherwise
		 */
		public boolean retryRequest(IOException exception, int executionCount, HttpContext context) {

			Throwable rootCause = ExceptionUtils.getRootCause(exception);
			logger.warn("XDM downloading attempt failed, root cause: {}", rootCause.toString());

			if (executionCount >= maxRetriesCount) {
				logger.warn("XDM downloading failed after {} retries in {} minute(s)",
						executionCount, maxDurationMinutes);
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
	}

	/**
	 * Custom ServiceUnavailableRetryStrategy implementation to retry on HTTP 503 (= service unavailable)
	 */
	private class CustomServiceUnavailableRetryStrategy implements ServiceUnavailableRetryStrategy {

		private final Logger logger             = LoggerFactory.getLogger(this.getClass());
		private final int    maxRetriesCount    = 3;
		private final int    maxDurationMinutes = 1;

		@Override
		public boolean retryRequest(final HttpResponse response, final int executionCount, final HttpContext context) {

			int httpStatusCode = response.getStatusLine().getStatusCode();
			if (httpStatusCode != 503)
				return false; // retry only on HTTP 503

			if (executionCount >= maxRetriesCount) {
				logger.warn("XDM downloading failed after {} retries in {} minute(s)",
						executionCount, maxDurationMinutes);
				return false;

			} else {
				logger.warn("XDM downloading attempt failed, HTTP status code: {}", httpStatusCode);
				return true;
			}
		}

		@Override
		public long getRetryInterval() {

			// Retry interval between subsequent requests, in milliseconds.
			// If not set, the default value is 1000 milliseconds.

			return DELAY_TO_RETRY_SECONDS * 1000;
		}
	}

	private static class HttpResponseHandler implements ResponseHandler<Path> {

		private Path targetFile;

		private HttpResponseHandler(Path targetFile) {
			this.targetFile = targetFile;
		}

		@Override
		public Path handleResponse(HttpResponse response) throws IOException {

			int httpStatusCode = response.getStatusLine().getStatusCode();
			if (httpStatusCode >= 300)
				throw new HttpResponseException(httpStatusCode, "HTTP error during file download");

			FileUtils.copyInputStreamToFile(response.getEntity().getContent(), targetFile.toFile()); // input stream is auto closed after
			return targetFile;
		}
	}
}
