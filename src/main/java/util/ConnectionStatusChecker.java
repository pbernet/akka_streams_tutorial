package util;

import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpOptions;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.channels.SocketChannel;
import java.util.Date;

/**
 * Inspired by:
 * https://mindchasers.com/dev/ping
 * <p>
 * No akka-streams here
 */
public class ConnectionStatusChecker {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionStatusChecker.class);
    private static final String TARGET_HOST = "google.com";

    public static void main(String[] args) {
        new ConnectionStatusChecker().run();
    }

    void run() {
        LOGGER.info("Scan using SocketChannel.connect...");
        final long timeToRespond = testSockets(TARGET_HOST, 80); // in milliseconds

        if (timeToRespond >= 0)
            LOGGER.info("...responded in: " + timeToRespond + " ms");
        else
            LOGGER.error("Failed");

        LOGGER.info("Scan using ping shell cmd...");
        try {
            boolean isSuccess = testPing(TARGET_HOST);
            if (isSuccess)
                LOGGER.info("...reachable via ping cmd");
            else
                LOGGER.info("...not reachable via ping cmd");
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
        LOGGER.info("Scan using HTTP OPTIONS request...");
//		SSLContexts sslContext = SSLContexts.custom()
//				.loadKeyMaterial()
//				.loadTrustMaterial()
//				.build();
        testHttp("https://" + TARGET_HOST, null);
    }

    /**
     * Connect using layer4 (sockets)
     *
     * @return delay in ms if the specified host responded, -1 if it failed
     */
    private long testSockets(String hostAddress, int port) {
        InetAddress inetAddress = null;
        InetSocketAddress socketAddress = null;
        long timeToRespond = -1;
        Date start, stop;

        try {
            inetAddress = InetAddress.getByName(hostAddress);
        } catch (UnknownHostException e) {
            LOGGER.warn("Unable to connect due to unknown host: ", e);
        }

        try {
            socketAddress = new InetSocketAddress(inetAddress, port);
        } catch (IllegalArgumentException e) {
            LOGGER.warn("Unable to connect, port may be invalid: ", e);
        }

        // Open the channel, set it to non-blocking, initiate connect
        try (SocketChannel sc = SocketChannel.open()) {
            sc.configureBlocking(true);
            start = new Date();
            if (sc.connect(socketAddress)) {
                stop = new Date();
                timeToRespond = (stop.getTime() - start.getTime());
            }
        } catch (IOException e) {
            LOGGER.info("Problem, connection could not be made: ", e);
        }
        return timeToRespond;
    }

    /**
     * Alternative: Connect using ping shell cmd
     *
     * @param hostAddress
     * @return true if the host is reachable
     * @throws IOException
     * @throws InterruptedException
     */
    private boolean testPing(String hostAddress) throws IOException, InterruptedException {
        String os = System.getProperty("os.name");
        String pingOption = (os.equals("Windows 10")) ? "n" : "c"; // 'c' for non-windows os

        Process p1 = java.lang.Runtime.getRuntime().exec("ping -" + pingOption + " 1 " + hostAddress);
        int returnVal = p1.waitFor();
        return returnVal == 0;
    }

    /**
     * Connect using HTTP(S) OPTIONS request
     *
     * @param endpointURL
     * @param sslContext Optional, helps to detect cert issues
     * @return true if the host is reachable
     */
    private boolean testHttp(String endpointURL, SSLContext sslContext) {
        try {
            HttpClient httpClient = HttpClients.custom().setSSLContext(sslContext).build();
            httpClient.execute(new HttpOptions(endpointURL));
            LOGGER.info("...successfully connected via HTTP OPTONS request to: {}", endpointURL);
            return true;
        } catch (Exception e) {
            LOGGER.warn("...unable to connect to: {}. Reason: ", endpointURL, e);
            throw new RuntimeException(e);
        }
    }
}
