package util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.channels.SocketChannel;
import java.util.Date;

/**
 * Inspired by:
 * https://mindchasers.com/dev/ping
 */
public class ConnectionStatusReport {
	private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionStatusReport.class);

	public static void main(String[] args) {
		new ConnectionStatusReport().run();
	}

	void run() {
		LOGGER.info("Scan using SocketChannel.connect:");
		final long timeToRespond = test("google.com", 80); // in milliseconds

		if (timeToRespond >= 0)
			LOGGER.info(" responded in: " + timeToRespond + " ms");
		else
			LOGGER.error("Failed");

		LOGGER.info("Scan using ping shell cmd");
		try {
			boolean isSuccess = testCmd("google.com");
			if (isSuccess)
				LOGGER.info("reachable via ping cmd");
			else
				LOGGER.info("not reachable via ping cmd");
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Connect using layer4 (sockets)
	 *
	 * @param
	 * @return delay if the specified host responded, -1 if failed
	 */
	private long test(String hostAddress, int port) {
		InetAddress inetAddress = null;
		InetSocketAddress socketAddress = null;
		SocketChannel sc = null;
		long timeToRespond = -1;
		Date start, stop;

		try {
			inetAddress = InetAddress.getByName(hostAddress);
		} catch (UnknownHostException e) {
			LOGGER.info("Problem, unknown host: ");
			e.printStackTrace();
		}

		try {
			socketAddress = new InetSocketAddress(inetAddress, port);
		} catch (IllegalArgumentException e) {
			LOGGER.info("Problem, port may be invalid: ");
			e.printStackTrace();
		}

		// Open the channel, set it to non-blocking, initiate connect
		try {
			sc = SocketChannel.open();
			sc.configureBlocking(true);
			start = new Date();
			if (sc.connect(socketAddress)) {
				stop = new Date();
				timeToRespond = (stop.getTime() - start.getTime());
			}
		} catch (IOException e) {
			LOGGER.info("Problem, connection could not be made: ");
			e.printStackTrace();
		}

		try {
			sc.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return timeToRespond;
	}

	/**
	 * Connect using shell cmd
	 *
	 * @param hostAddress
	 * @return true if the host is reachable
	 * @throws IOException
	 * @throws InterruptedException
	 */

	private boolean testCmd(String hostAddress) throws IOException, InterruptedException {
		// in case of Linux change the 'n' to 'c'
		String os = System.getProperty("os.name");
		String pingOption = (os.equals("Windows 10")) ? "n" : "c";

		Process p1 = java.lang.Runtime.getRuntime().exec("ping -" + pingOption + " 1 " + hostAddress);
		int returnVal = p1.waitFor();
		return returnVal==0;
	}
}
