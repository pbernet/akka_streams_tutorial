package alpakka.tcp_to_websockets.hl7mllp;

import ca.uhn.hl7v2.HL7Exception;
import ca.uhn.hl7v2.protocol.ReceivingApplicationExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Hook for additional error processing depending on exception type or the exception message
 * Basic error processing should be done in {@link HL7Exception}
 *
 */
public class ExceptionHandler implements ReceivingApplicationExceptionHandler {
	private static final Logger LOGGER = LoggerFactory.getLogger(ExceptionHandler.class);

	@Override
	/*
	 * @return theOutgoingNegativeAcknowledgementMessage (possibly enhanced)
	 */
	public String processException(String theIncomingMessage, Map<String, Object> theIncomingMetadata, String theOutgoingNegativeAcknowledgementMessage, Exception theException)
			throws HL7Exception {

		LOGGER.error("The error message was:" + theException.getMessage() + "\n");

		return theOutgoingNegativeAcknowledgementMessage;
	}
}
