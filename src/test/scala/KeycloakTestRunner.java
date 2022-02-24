import dasniko.testcontainers.keycloak.KeycloakContainer;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Created issue in:
 * https://github.com/dasniko/testcontainers-keycloak/issues
 */
@Testcontainers
public class KeycloakTestRunner {

    // TODO The quarkus based container fails with container log msgs:
    // Unknown options: '-c', 'standalone.xml', '-b', '0.0.0.0'
    // Possible solutions: -cf, --config-file
    // Try 'kc.sh --help' for more information on the available options.
    @Container
    KeycloakContainer keycloak = new KeycloakContainer("quay.io/keycloak/keycloak:17.0.0");

    @Test
    public void test() {
    }
}
