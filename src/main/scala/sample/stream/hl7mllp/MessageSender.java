package sample.stream.hl7mllp;

import ca.uhn.hl7v2.DefaultHapiContext;
import ca.uhn.hl7v2.HL7Exception;
import ca.uhn.hl7v2.HapiContext;
import ca.uhn.hl7v2.app.Connection;
import ca.uhn.hl7v2.app.Initiator;
import ca.uhn.hl7v2.llp.LLPException;
import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.parser.PipeParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class MessageSender {
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageSender.class);
    private static final MessageSender instance = new MessageSender();

    private MessageSender() {

    }

    public static MessageSender getInstance() {
        return instance;
    }

    public MessageSender sendMessage(String message, String host, int port) throws HL7Exception, IOException, LLPException {
        return sendMessage(message, host, port, false);
    }

    /**
     * Sends a message to the specified host and port
     *
     * @param inputMessage The message to send to the middleware
     * @param host         The host where the middleware resides
     * @param port         The listening port of the middleware
     * @param useTls       Whether to use SSL/TLS for the communication with the middleware
     * @return the LLPMessageTester object for chaining
     * @throws HL7Exception
     * @throws IOException
     * @throws LLPException
     */
    public MessageSender sendMessage(String inputMessage, String host, int port, boolean useTls) throws HL7Exception, IOException, LLPException {
        LOGGER.info("Attempting to send the following message...\n" + inputMessage);
        HapiContext context = new DefaultHapiContext();
        // Set up a parser using the validation rules
        //context.setValidationRuleBuilder(builder);
        PipeParser parser = context.getPipeParser();
        Message message = parser.parse(inputMessage);

        // A connection object represents a socket attached to an HL7 server
        Connection connection = null;
        try {
            connection = context.newClient(host, port, useTls);

            // The initiator is used to transmit unsolicited messages
            Initiator initiator = connection.getInitiator();
            Message response = initiator.sendAndReceive(message);

            String responseString = parser.encode(response);
            LOGGER.info("Received response:");
            LOGGER.info(responseString);
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
        return this;
    }
}