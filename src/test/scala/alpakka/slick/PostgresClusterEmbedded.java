package alpakka.slick;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Wrapper around <ahref="https://github.com/zonkyio/embedded-postgres">embedded-postgres</a>.
 * <p>
 * Integrated into unit tests with @RegisterExtension
 */
public class PostgresClusterEmbedded implements BeforeAllCallback, AfterAllCallback {
    private EmbeddedPostgres postgres;

    @Override
    public void beforeAll(ExtensionContext context) throws IOException {
        postgres =
                EmbeddedPostgres.builder()
                        // Explicitly clean-up the data directory to always start with a clean setup
                        .setCleanDataDirectory(true)
                        .setPort(5432)
                        // Set the write-ahead log level to "logical" to make sure that enough
                        // information is written
                        // to the write-ahead log as is required for CDC with Debezium
                        .setServerConfig("wal_level", "logical")
                        .start();
    }

    @Override
    public void afterAll(ExtensionContext context) throws IOException {
        postgres.close();
    }

    public void executeStatement(String statement) throws SQLException {
        Connection conn = postgres.getPostgresDatabase().getConnection();
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(statement);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public String getJdbcUrl() {
        return postgres.getJdbcUrl("test", "test");
    }
}
