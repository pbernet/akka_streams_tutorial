package alpakka.tcp_to_websockets.hl7mllp;

import ca.uhn.hl7v2.DefaultHapiContext;
import ca.uhn.hl7v2.HapiContext;
import ca.uhn.hl7v2.app.Connection;
import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.util.Hl7InputStreamMessageIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.stream.IntStream;


/**
 * Basic sender, which loops through ML7 messages read from file
 *
 */
public class Hl7TcpClientClassic {
    private static final Logger LOGGER = LoggerFactory.getLogger(Hl7TcpClientClassic.class);
    private static HapiContext context = new DefaultHapiContext();

    private static final int PORT_NUMBER = 6160;

    public static void main(String[] args) {
        IntStream.range(0, 100).forEach(each -> processMessages());
    }

    private static void processMessages() {
        Connection connectionWithServer = null;
        try {
            FileReader reader = new FileReader("src/main/resources/ADT_ORM_Hl7Messages.txt");
            Hl7InputStreamMessageIterator messageIterator = new Hl7InputStreamMessageIterator(reader);

            while (messageIterator.hasNext()) {

                Thread.sleep(1000);

                if (connectionWithServer == null) {
                    boolean useSecureTlsConnection = false;
                    connectionWithServer = context.newClient("localhost", PORT_NUMBER, useSecureTlsConnection);
                }

                try {
                    Message nextMessage = messageIterator.next();

                    LOGGER.info("About to send message: " + stripMessage(nextMessage.encode()));
                    Message messageResponse = connectionWithServer.getInitiator().sendAndReceive(nextMessage);

                    // TODO Check for NACK response and maybe retry
                    LOGGER.info("Response: " + stripMessage(messageResponse.encode()));
                } catch (IOException e) {
                    // TODO handle connection exceptions and retry, currently in-flight message is lost
                    LOGGER.error("Inner Ex during message processing:",  e);
                    // At least a new tcp connection will used for the next message
                    connectionWithServer.close();
                    connectionWithServer = null;
                }
            }

        } catch (Exception e) {
            LOGGER.error("Outer Ex during message processing:",  e);
        } finally {
            if (connectionWithServer != null) connectionWithServer.close();
        }
    }

    private static String stripMessage(String message) {
        String strippedMessage = message.replace("\r", "\n");
        StringTokenizer tokenizer = new StringTokenizer(strippedMessage, "\n");
        List<StringBuilder> strippedTokens = new ArrayList<>();
        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();
            StringBuilder tokenBuilder = new StringBuilder(token);
            while (tokenBuilder.charAt(tokenBuilder.length() - 1) == '|') {
                tokenBuilder.deleteCharAt(tokenBuilder.length() - 1);
            }
            strippedTokens.add(tokenBuilder);
        }
        StringBuilder strippedMessageBuilder = new StringBuilder();
        for (StringBuilder token : strippedTokens) {
            strippedMessageBuilder.append(token);
        }

        return strippedMessageBuilder.toString();
    }
}
