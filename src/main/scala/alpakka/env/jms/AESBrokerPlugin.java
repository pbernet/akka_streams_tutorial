package alpakka.env.jms;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerPluginSupport;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.Security;

/**
 * Inspired by:
 * https://github.com/justinreock-roguewave/activemq-aes-plugin
 *
 * TODO implement hooks for hooks for start/stop
 *
 *
 */
public class AESBrokerPlugin extends BrokerPluginSupport {
    private static final Logger LOGGER = LoggerFactory.getLogger(AESBrokerPlugin.class);

    public Broker installPlugin(Broker broker) {
        LOGGER.info("About to install AES payload encryption plugin, using SecurityProvider BC (Bouncy Castle)");

        Security.addProvider(new BouncyCastleProvider());
        AESBroker aesBroker = null;
        try {
            aesBroker = new AESBroker(broker);
            //Because of race condition: preProcessDispatch is called before AESBroker is initialized
            Thread.sleep(1000);
        } catch (Exception e) {
            LOGGER.error("Exception during installation AES encryption plugin: ", e);
        }

        LOGGER.info("Successfully installed AES payload encryption plugin");
        return aesBroker;
    }
}