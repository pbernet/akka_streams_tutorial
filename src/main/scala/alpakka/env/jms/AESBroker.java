package alpakka.env.jms;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageDispatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import javax.jms.JMSException;
import javax.xml.bind.DatatypeConverter;
import java.security.Key;
import java.util.Base64;

/**
 * Uses AES 256 "AES/CBC/PKCS5PADDING" with empty IV for now
 *
 */
public class AESBroker extends BrokerFilter {
    private static final Logger LOGGER = LoggerFactory.getLogger(AESBroker.class);

    private final String keyStr = System.getProperty("activemq.aeskey");
    private Key aesKey = null;
    private Cipher cipher = null;

    public AESBroker(Broker next) throws Exception {
        super(next);
        init();
    }

    private void init() throws Exception {
        if (keyStr == null || keyStr.length() != 16) {
            throw new Exception("Bad AES key configured - ensure that JVM system property 'activemq.aeskey' is set to a 16 character string");
        }
        aesKey = new SecretKeySpec(keyStr.getBytes(), "AES");
        cipher = Cipher.getInstance("AES/CBC/PKCS5PADDING");
    }

    public String encrypt(String text) throws Exception {
        cipher.init(Cipher.ENCRYPT_MODE, aesKey, new IvParameterSpec(new byte[16]));
        return toHexString(cipher.doFinal(text.getBytes()));
    }

    public String decrypt(String text) throws Exception {
        cipher.init(Cipher.DECRYPT_MODE, aesKey, new IvParameterSpec(new byte[16]));
        return new String(cipher.doFinal(toByteArray(text)));
    }

    public String toHexString(byte[] array) {
        return DatatypeConverter.printHexBinary(array);
    }

    public byte[] toByteArray(String s) {
        return DatatypeConverter.parseHexBinary(s);
    }

    public Message encryptMessage(Message msg) {

        String msgBody = "";
        ActiveMQTextMessage tm = initializeTextMessage(msg);
        try {
            msgBody = tm.getText();
        }
        catch (JMSException e) {
            LOGGER.error("Could not get message body contents for encryption \n" + e.getMessage());
            return msg;
        }

        try {
            msgBody = encrypt(msgBody);
            msgBody = Base64.getEncoder().encodeToString(msgBody.getBytes());
        }
        catch (Exception e) {
            LOGGER.error("Could not encrypt message\n", e);
            return msg;
        }

        LOGGER.debug("Encrypted message to: " + msgBody);

        try {
            tm.setText(msgBody);
        }
        catch (Exception e) {
            LOGGER.error("Could not write to message body\n", e);
            return msg;
        }

        return tm;
    }

    public Message decryptMessage(Message msg) {

        String msgBody = "";
        ActiveMQTextMessage tm = initializeTextMessage(msg);
        try {
            msgBody = tm.getText();
        }
        catch (JMSException e) {
            LOGGER.error("Could not get message body contents for decryption \n", e);
            return msg;
        }

        try {
            msgBody = new String(Base64.getDecoder().decode(msgBody),"utf-8");
            msgBody = decrypt(msgBody);
        }
        catch (Exception e) {
            LOGGER.error("Could not decrypt message\n", e);
            return msg;
        }

        LOGGER.debug("Decrypted message to: " + msgBody);

        try {
            tm.setText(msgBody);
        }
        catch (Exception e) {
            LOGGER.error("Could not write to message body\n" + e.getMessage());
            return msg;
        }

        return tm;
    }

    private ActiveMQTextMessage initializeTextMessage(Message msg) {
        ActiveMQTextMessage tm = (ActiveMQTextMessage) msg.getMessage();
        tm.setReadOnlyBody(false);
        return tm;
    }

    public void send(ProducerBrokerExchange producerExchange, Message messageSend) throws Exception {
        if (messageSend instanceof ActiveMQTextMessage) {
            ActiveMQTextMessage encryptedMessage = (ActiveMQTextMessage) encryptMessage(messageSend.getMessage());
            next.send(producerExchange, encryptedMessage);
        }
    }

    public void preProcessDispatch(MessageDispatch messageDispatch) {
        if (messageDispatch.getMessage() instanceof ActiveMQTextMessage) {
            ActiveMQTextMessage encryptedMessage = (ActiveMQTextMessage) messageDispatch.getMessage();
            ActiveMQTextMessage decryptedMessage = (ActiveMQTextMessage) decryptMessage(encryptedMessage);
            messageDispatch.setMessage(decryptedMessage);
            next.preProcessDispatch(messageDispatch);
        }
    }
}