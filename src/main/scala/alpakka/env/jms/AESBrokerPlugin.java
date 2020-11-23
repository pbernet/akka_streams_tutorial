package alpakka.env.jms;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerPlugin;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.Security;

/**
 * Inspired by:
 * https://github.com/justinreock-roguewave/activemq-aes-plugin
 *
 */
public class AESBrokerPlugin implements BrokerPlugin {
    private static final Logger LOGGER = LoggerFactory.getLogger(AESBrokerPlugin.class);

    public Broker installPlugin(Broker broker) {
        LOGGER.info("About to install AES payload encryption plugin, using SecurityProvider BC (Bouncy Castle)");

        Security.addProvider(new BouncyCastleProvider());
        AESBroker aesBroker = null;
        try {
            aesBroker = new AESBroker(broker);
        } catch (Exception e) {
            LOGGER.error("Exception during installation AES encryption plugin: ", e);
        }
        LOGGER.info("Successfully installed AES payload encryption plugin");
        return aesBroker;
    }
}