package alpakka.tcp_to_websockets.hl7mllp;

import ca.uhn.hl7v2.DefaultHapiContext;
import ca.uhn.hl7v2.HL7Exception;
import ca.uhn.hl7v2.HapiContext;
import ca.uhn.hl7v2.app.Connection;
import ca.uhn.hl7v2.app.HL7Service;
import ca.uhn.hl7v2.app.Initiator;
import ca.uhn.hl7v2.llp.LLPException;
import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.model.v24.message.ADT_A01;
import ca.uhn.hl7v2.parser.Parser;
import ca.uhn.hl7v2.protocol.ApplicationRouter.AppRoutingData;
import ca.uhn.hl7v2.protocol.impl.AppRoutingDataImpl;

import java.io.IOException;


/**
 * Classic HL7 MLLP tcp listener, used as a reference
 *
 */
public class Hl7MllpListener {

    private static final int PORT_NUMBER = 6160;

    private static HapiContext context = new DefaultHapiContext();

    public static void main(String[] args) {

        try {
            boolean useSecureConnection = false; // are you using TLS/SSL?

            // The ValidationContext is used during parsing and well as during
            // validation using {@link ca.uhn.hl7v2.validation.Validator} objects.
            // Set to false to do parsing without validation
            //context.getParserConfiguration().setValidating(false);

            // Set to false if there is a separate validation step
            //context.setValidationContext(ValidationContextFactory.noValidation());

            // HL7 server

            HL7Service ourHl7Server = context.newServer(PORT_NUMBER, useSecureConnection);

            // routing rules for your HL7 listener, all conditions must cumulatively be true

            // ADT
            AppRoutingDataImpl adtRouter = new AppRoutingDataImpl("ADT", "A0.", "*",
                     "2.5.1");
            // ORM
            AppRoutingDataImpl ormRouter = new AppRoutingDataImpl("ORM", "O01", "*",
             "2.5.1");

            ourHl7Server.registerApplication(adtRouter, new MockApp());
            ourHl7Server.registerApplication(ormRouter, new MockApp());

            ourHl7Server.setExceptionHandler(new ExceptionHandler());
            ourHl7Server.startAndWait();


            // HL7 local test client
            //hl7Client(useSecureConnection);

            //ourHl7Server.stopAndWait();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void hl7Client(boolean useSecureConnection) throws HL7Exception, IOException, LLPException {

        // Set up a connection and a initiator
        Connection ourConnection = context.newLazyClient("localhost", PORT_NUMBER, useSecureConnection);
        Initiator initiator = ourConnection.getInitiator();


        // assemble a test message to send to our listener
        ADT_A01 adtMessage = (ADT_A01) AdtMessageFactory.createMessage("A01");

        // send this test message through our test client's initiator and get a response
        Message messageResponse = initiator.sendAndReceive(adtMessage);

        // parse the message response
        Parser ourPipeParser = context.getPipeParser();
        String responseString = ourPipeParser.encode(messageResponse);
        System.out.println("Received a message response:\n" + responseString);

        // close our test connection
        ourConnection.close();
    }

}

class RegistrationEventRoutingData implements AppRoutingData {

    // Note, all conditions must cumulatively be true
    // for a message to be processed

    @Override
    public String getVersion() {
        // process HL7 2.5.1 version messages only
        return "2.5.1";
    }

    @Override
    public String getTriggerEvent() {
        // you can use regular expression-based matching for your routing
        // only trigger events that start with 'A0' will be processed
        return "A0.";
    }

    @Override
    public String getProcessingId() {
        // process all messages regardless of processing id
        return "*";
    }

    @Override
    public String getMessageType() {
        // process only ADT message types
        return "ADT";
    }
}
