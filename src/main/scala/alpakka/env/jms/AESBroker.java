package alpakka.env.jms;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageDispatch;
import org.apache.commons.compress.utils.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import javax.jms.JMSException;
import javax.xml.bind.DatatypeConverter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.Key;
import java.security.SecureRandom;
import java.util.Base64;

/**
 * Uses AES 256 "AES/CBC/PKCS5PADDING"
 */
public class AESBroker extends BrokerFilter {
    private static final Logger LOGGER = LoggerFactory.getLogger(AESBroker.class);

    private static final String IS_ENCRYPTED = "isEncrypted";
    private static final String KEY_STRING = System.getProperty("activemq.aeskey");
    private static final int IV_LENGTH = 16;

    private Key aesKey;

    public AESBroker(Broker next) throws Exception {
        super(next);
        init();
    }

    private void init() throws Exception {
        if (KEY_STRING == null || KEY_STRING.length() != IV_LENGTH) {
            throw new Exception("Bad AES key configured - ensure that JVM system property 'activemq.aeskey' is set to a 16 character string");
        }
        aesKey = new SecretKeySpec(KEY_STRING.getBytes(), "AES");
    }

    public String encrypt(String text) throws Exception {
        Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5PADDING", "BC");
        byte [] iv = genInitialisationVector();
        cipher.init(Cipher.ENCRYPT_MODE, aesKey, new IvParameterSpec(iv));

        // Prepend IV
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        outputStream.write(iv);
        outputStream.write(cipher.doFinal(text.getBytes()));

        return toHexString(outputStream.toByteArray());
    }

    public String decrypt(String text) throws Exception {
        Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5PADDING", "BC");

        // Read IV (first n bytes from payload)
        byte[] ivBytesBuffer = new byte[IV_LENGTH];
        InputStream payload = new ByteArrayInputStream(toByteArray(text));
        payload.read(ivBytesBuffer);

        cipher.init(Cipher.DECRYPT_MODE, aesKey, new IvParameterSpec(ivBytesBuffer));
        return new String(cipher.doFinal(IOUtils.toByteArray(payload)));
    }

    public String toHexString(byte[] array) {
        return DatatypeConverter.printHexBinary(array);
    }

    public byte[] toByteArray(String s) {
        return DatatypeConverter.parseHexBinary(s);
    }

    public Message encryptMessage(Message msg) {
        LOGGER.debug("About to encrypt message with id: {}", msg.getCorrelationId());

        String msgBodyOriginal;
        String msgBodyEncrypted;
        ActiveMQTextMessage tm = initializeTextMessage(msg);
        try {
            msgBodyOriginal = tm.getText();
        } catch (JMSException e) {
            LOGGER.error("Could not get message body contents for encryption. Cause:", e);
            return msg;
        }

        try {
            msgBodyEncrypted = Base64.getEncoder().encodeToString(encrypt(msgBodyOriginal).getBytes());
        } catch (Exception e) {
            LOGGER.error("Could not encrypt message with id: {}. Cause:", tm.getCorrelationId(), e);
            return msg;
        }

        LOGGER.debug("Successfully encrypted message to:" + msgBodyEncrypted);

        try {
            tm.setText(msgBodyEncrypted);
            tm.setProperty(IS_ENCRYPTED, true);
        } catch (Exception e) {
            LOGGER.error("Could not write to message body. Cause:", e);
            return msg;
        }
        return tm;
    }

    public Message decryptMessage(Message msg) {
        LOGGER.debug("About to decrypt message with id: {}", msg.getCorrelationId());

        String msgBodyOriginal;
        String msgBodyDecrypted;
        Boolean isEncrypted;
        ActiveMQTextMessage tm = initializeTextMessage(msg);

        try {
            isEncrypted = (Boolean) tm.getProperty(IS_ENCRYPTED);
        } catch (IOException e) {
            LOGGER.error("Could not read metadata attribute {}. Cause:", IS_ENCRYPTED, e);
            return msg;
        }

        if (isEncrypted) {
            try {
                msgBodyOriginal = tm.getText();
                LOGGER.debug("About to decrypt message with id: {} and content: {}", msg.getCorrelationId(), msgBodyOriginal);
            } catch (JMSException e) {
                LOGGER.error("Could not get message body contents for decryption. Cause:", e);
                return msg;
            }

            try {
                msgBodyDecrypted = decrypt(new String(Base64.getDecoder().decode(msgBodyOriginal), StandardCharsets.UTF_8));
            } catch (Exception e) {
                LOGGER.error("Could not decrypt message with id: {}. Cause:", tm.getCorrelationId(), e);
                return msg;
            }

            LOGGER.debug("Successfully decrypted message to: " + msgBodyDecrypted);

            try {
                tm.setText(msgBodyDecrypted);
                tm.setProperty(IS_ENCRYPTED, false);
            } catch (Exception e) {
                LOGGER.error("Could not write to message body. Cause:", e);
                return msg;
            }

            return tm;
        } else {
            LOGGER.debug("Can not decrypt message with id: {}, because it is already decrypted", msg.getCorrelationId());
            return msg;
        }
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

    // Synonym for IV would be: nonce (= number once)
    private byte [] genInitialisationVector() {
        return new SecureRandom().generateSeed(IV_LENGTH);
    }
}