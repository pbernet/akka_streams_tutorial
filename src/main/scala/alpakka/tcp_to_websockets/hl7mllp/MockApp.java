package alpakka.tcp_to_websockets.hl7mllp;

import ca.uhn.hl7v2.DefaultHapiContext;
import ca.uhn.hl7v2.HL7Exception;
import ca.uhn.hl7v2.HapiContext;
import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.model.Segment;
import ca.uhn.hl7v2.protocol.ReceivingApplication;
import ca.uhn.hl7v2.util.Terser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * Mock application that returns default ACK messages.
 */
public class MockApp implements ReceivingApplication<Message> {

    private static HapiContext context = new DefaultHapiContext();

    /**
     * The parsing is done in ApplicationRouterImpl
     * Returns a default ack message.
     */
    public Message processMessage(Message in, Map theMetadata) throws HL7Exception {

        Message response = null;
        try {

            // Metadata contains raw message as well
            System.out.println("Received from: " + theMetadata.get("SENDING_IP") + ":" + theMetadata.get("SENDING_PORT"));

            String receivedEncodedMessage = context.getPipeParser().encode(in);

            String strippedMessage = stripMessage(receivedEncodedMessage);

            System.out.println("Incoming message version: " + in.getVersion() + " size: " + strippedMessage.length() + " payload: " + strippedMessage);
            
            // TODO How to convert this gracefully to JSON like structure?
            // 1)
            Terser tersed = new Terser(in);
            Segment msh = tersed.getSegment("MSH");
            System.out.println("Tersed MSH: " + msh.getName() + "\n\n");

            // 2) Possible to cast
            // ADT_A01 adtMessage = (ADT_A01) in;

            // 3)
            // https://github.com/amesar/hl7-json-spark
            // https://camel.apache.org/components/latest/dataformats/hl7-dataformat.html

            response = in.generateACK();
        } catch (HL7Exception | IOException e) {
            e.printStackTrace();
        }

        Terser t = new Terser(response);
        t.set("MSA-3", "this is a mock application response");
        return response;
    }

    private String stripMessage(String message) {
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


    /**
     * Returns true.
     */
    public boolean canProcess(Message in) {
        return true;
    }

}
