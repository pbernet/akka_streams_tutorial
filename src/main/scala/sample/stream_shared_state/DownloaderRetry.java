package sample.stream_shared_state;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.ServiceUnavailableRetryStrategy;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.protocol.HttpContext;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;


/**
 * TODO Add Backoff:
 * https://stackoverflow.com/questions/35391005/how-to-use-an-exponential-backoff-strategy-with-apache-httpclient
 *
 */
public class DownloaderRetry {

    public static void main(String[] args) throws Exception {

        //URL url = new URL("https://www.baeldung.com/java-url");
        URL url = new URL("http://httpstat.us/503");
        File resFile = new DownloaderRetry().download(url, new File("download.tmp"));
        System.out.print("Downloaded file: " + resFile.getName() + " with size: " + resFile.length() + " bytes");
    }


    private File download(URL url, File dstFile) {
        PoolingHttpClientConnectionManager manager = new PoolingHttpClientConnectionManager();
        manager.setDefaultMaxPerRoute(20);
        manager.setMaxTotal(200);

        CloseableHttpClient httpclient = HttpClients.custom()
                .setConnectionManager(manager)
                .setServiceUnavailableRetryStrategy(new ServiceUnavailableRetryStrategy() {
                    @Override
                    public boolean retryRequest(
                            final HttpResponse response, final int executionCount, final HttpContext context) {
                        int statusCode = response.getStatusLine().getStatusCode();
                        System.out.println("Got: " + statusCode + " on attempt: " + executionCount);
                        //TODO Add other Status code for Retry
                        return statusCode == 503 && executionCount < 5;
                    }

                    @Override
                    public long getRetryInterval() {
                        return 0;
                    }
                })
                .build();

        try {
            HttpGet get = new HttpGet(url.toURI());
            return httpclient.execute(get, new FileDownloadResponseHandler(dstFile));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            IOUtils.closeQuietly(httpclient);
        }
    }

    static class FileDownloadResponseHandler implements ResponseHandler<File> {

        private final File target;

        private FileDownloadResponseHandler(File target) {
            this.target = target;
        }

        @Override
        public File handleResponse(HttpResponse response) throws  IOException {
            InputStream source = response.getEntity().getContent();
            FileUtils.copyInputStreamToFile(source, this.target);
            return this.target;
        }
    }
}
