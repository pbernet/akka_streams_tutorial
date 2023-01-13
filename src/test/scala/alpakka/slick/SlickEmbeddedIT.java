package alpakka.slick;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.jdk.javaapi.FutureConverters;

import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Same as SlickIT, but with local embedded Postgres DB
 */
class SlickEmbeddedIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(SlickEmbeddedIT.class);
    private static SlickRunner SLICK_RUNNER;

    @RegisterExtension
    private static final PostgresClusterEmbedded POSTGRES = new PostgresClusterEmbedded();

    private static final String DB_NAME = "test";
    private static final String USER_NAME = "test";
    private static final String PASSWORD = "test";

    @BeforeEach
    void setUp() throws SQLException {
        POSTGRES.executeStatement("CREATE DATABASE " + DB_NAME);
        POSTGRES.executeStatement("CREATE USER " + USER_NAME + " with encrypted PASSWORD '" + PASSWORD + "'");
        POSTGRES.executeStatement("grant all privileges on database " + DB_NAME + " to " + USER_NAME);

        SLICK_RUNNER = SlickRunner.apply(POSTGRES.getJdbcUrl());
        SLICK_RUNNER.createTableOnSession();
        LOGGER.info("DB: {} created at URL: {}, USER_NAME: {}, PASSWORD: {}", DB_NAME, POSTGRES.getJdbcUrl(), USER_NAME, PASSWORD);
    }

    @AfterEach
    void teardown() throws SQLException {
        SLICK_RUNNER.dropTableOnSession();
        SLICK_RUNNER.terminate();
        POSTGRES.executeStatement("DROP DATABASE " + DB_NAME);
        POSTGRES.executeStatement("DROP USER " + USER_NAME);
    }

    @Test
    void populateAndReadUsers() {
        int noOfUsers = 100;
        SLICK_RUNNER.populateSync(noOfUsers);
        assertThat(SLICK_RUNNER.readUsersSync().size()).isEqualTo(noOfUsers);
    }

    @Test
    void populateAndReadUsersPaged() {
        int noOfUsers = 20000;
        SLICK_RUNNER.populateSync(noOfUsers);

        // Done via counter to avoid Scala->Java result collection conversion "type trouble"
        assertThat(FutureConverters.asJava(SLICK_RUNNER.processUsersPaged())).succeedsWithin(5, TimeUnit.SECONDS);
        assertThat(SLICK_RUNNER.counter().get()).isEqualTo(noOfUsers);
    }

    @Test
    void populateAndCountUsers() {
        int noOfUsers = 100;
        SLICK_RUNNER.populateSync(noOfUsers);
        assertThat(SLICK_RUNNER.getTotal()).isEqualTo(noOfUsers);
    }
}