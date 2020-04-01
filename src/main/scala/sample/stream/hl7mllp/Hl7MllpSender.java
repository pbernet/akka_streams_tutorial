package sample.stream.hl7mllp;

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


/**
 * Basic sender, which reads ML7 messages from file
 * Is responsible for closing tcp connection at the end
 */
public class Hl7MllpSender {
    private static final Logger LOGGER = LoggerFactory.getLogger(Hl7MllpSender.class);
    private static HapiContext context = new DefaultHapiContext();

    // TODO make configurable
    private static final int PORT_NUMBER = 6160;

    public static void main(String[] args) {

        Connection connectionWithServer = null;

        try {

            FileReader reader = new FileReader("/Users/Shared/projects/akka_streams_tutorial/src/main/resources/ADT_ORM_Hl7Messages.txt");
            Hl7InputStreamMessageIterator messageIterator = new Hl7InputStreamMessageIterator(reader);

            while (messageIterator.hasNext()) {

                if (connectionWithServer == null) {
                    boolean useSecureTlsConnection = false;
                    connectionWithServer = context.newClient("localhost", PORT_NUMBER, useSecureTlsConnection);
                }

                try {
                    Message nextMessage = messageIterator.next();
                    Message messageResponse = connectionWithServer.getInitiator().sendAndReceive(nextMessage);

                    String strippedMessage = stripMessage(messageResponse.encode());
                    LOGGER.info("Response: " + strippedMessage);
                } catch (IOException e) {
                    // TODO handle exceptions and retry, currently faulty message will be lost
                    e.printStackTrace();
                    // At least a new connection is used for the next message
                    connectionWithServer.close();
                    connectionWithServer = null;
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
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
