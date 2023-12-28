package alpakka.clickhousedb;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.clickhouse.ClickHouseContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import scala.jdk.javaapi.FutureConverters;

import javax.sql.DataSource;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/**
 * ClickhouseDB is a read optimized DB
 * <p>
 * This IT test uses the JDBC client as well as the Scala client
 * We keep the test suite running after the last test,
 * so we can inspect the DB via the Tabix Browser client
 * Doc:
 * https://clickhouse.com
 * Good overview when to use/NOT to use:
 * https://aiven.io/blog/what-is-clickhouse
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Testcontainers
public class ClickhousedbIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClickhousedbIT.class);
    private static final Integer CLICKHOUSEDB_HTTP_PORT = 8123;

    public String searchAfterPattern;

    // Doc: https://java.testcontainers.org/modules/databases/clickhouse/
    @org.testcontainers.junit.jupiter.Container
    public static ClickHouseContainer clickhouseDBContainer = new ClickHouseContainer("clickhouse/clickhouse-server")
            .withUsername("test")
            .withPassword("test")
            .withDatabaseName("test")
            .withUrlParam("max_result_rows", "5");


    @BeforeAll
    public static void setupBeforeClass() throws IOException, InterruptedException {
        LOGGER.info("ClickhouseDB container listening on HTTP port: {}. Running: {} ", clickhouseDBContainer.getMappedPort(CLICKHOUSEDB_HTTP_PORT), clickhouseDBContainer.isRunning());

        Thread.sleep(1000);
        GenericContainer tabixWebClientContainer = new GenericContainer<>(DockerImageName.parse("spoonest/clickhouse-tabix-web-client"))
                .withExposedPorts(80)
                .withEnv("CH_LOGIN", "test")
                .withEnv("CH_PASSWORD", "test")
                .withEnv("CH_HOST", "localhost:" + clickhouseDBContainer.getMappedPort(CLICKHOUSEDB_HTTP_PORT));
        tabixWebClientContainer.start();
        LOGGER.info("Clickhouse Tabix UI listening on HTTP port: {}. Running: {} ", tabixWebClientContainer.getMappedPort(80), tabixWebClientContainer.isRunning());

        browserClient(tabixWebClientContainer.getMappedPort(80));
    }

    @AfterAll
    public static void shutdown() throws InterruptedException {
        LOGGER.info("Sleep to keep ClickhouseDB instance running...");
        Thread.sleep(10000000);
    }

    @BeforeEach
    public void setupBeforeTest(TestInfo testInfo) throws SQLException {
        searchAfterPattern = String.format("Starting test: %s", testInfo.getTestMethod().toString());
        LOGGER.info(searchAfterPattern);
    }

    @Test
    @Order(1)
    void testHealthHTTP() throws IOException {
        // Doc: https://clickhouse.com/docs/en/interfaces/http
        Integer httpPort = clickhouseDBContainer.getMappedPort(CLICKHOUSEDB_HTTP_PORT);
        URL url = new URL("http://localhost:" + httpPort + "/ping");
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod("GET");
        assertThat(con.getResponseCode()).isEqualTo(200);
    }

    @Test
    @Order(2)
    void testReadJDBC() throws SQLException {
        ResultSet resultSet = performQuery(clickhouseDBContainer, clickhouseDBContainer.getTestQueryString());
        int resultSetInt = resultSet.getInt(1);
        assertThat(resultSetInt).isEqualTo(1);
    }

    @Test
    @Order(3)
    void testWriteJDBC() throws SQLException {
        createTable();

        String newLine = System.getProperty("line.separator");
        String insertDataTextBlock =
                "INSERT INTO test.my_table (myfloat_nullable, mystr, myint_id)"
                        + newLine
                        + "VALUES (1.0, '1', 1), (2.0, '2', 2), (3.0, '3', 3)";


        Integer resultInsert = performStatement(clickhouseDBContainer, insertDataTextBlock);
        LOGGER.info("Successfully inserted: {} records via JDBC", resultInsert);
        dropTable();

        assertThat(resultInsert).isEqualTo(3);
    }

    @Test
    @Order(4)
    void testReadScala() {
        ClickhouseDB instance = ClickhouseDB.apply(clickhouseDBContainer.getMappedPort(CLICKHOUSEDB_HTTP_PORT));
        assertThat(instance.testRead()).isEqualTo("1");
    }

    @Test
    @Order(5)
    void testWriteReadScalaStreamed() throws SQLException {
        createTable();

        int noOfRecords = 100;
        ClickhouseDB instance = ClickhouseDB.apply(clickhouseDBContainer.getMappedPort(CLICKHOUSEDB_HTTP_PORT));
        FutureConverters.asJava(instance.writeAll(noOfRecords)).thenAccept(each ->
                {
                    assertThat(instance.countRows()).isEqualTo(noOfRecords);
                    assertThat(instance.readAllSource()).isEqualTo(noOfRecords);
                    assertThat(instance.readAllSourceByteString()).isEqualTo(noOfRecords);
                }
        );

        // since this is the last test we don't drop the table

    }

    protected ResultSet performQuery(JdbcDatabaseContainer<?> container, String sql) throws SQLException {
        DataSource ds = getDataSource(container);
        Statement statement = ds.getConnection().createStatement();
        statement.execute(sql);
        ResultSet resultSet = statement.getResultSet();

        resultSet.next();
        return resultSet;
    }

    protected int performStatement(JdbcDatabaseContainer<?> container, String sqlStatement) throws SQLException {
        DataSource ds = getDataSource(container);
        try (Statement statement = ds.getConnection().createStatement()) {
            return statement.executeUpdate(sqlStatement);
        }
    }

    protected DataSource getDataSource(JdbcDatabaseContainer<?> container) {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(container.getJdbcUrl());
        hikariConfig.setUsername(container.getUsername());
        hikariConfig.setPassword(container.getPassword());
        hikariConfig.setDriverClassName(container.getDriverClassName());
        return new HikariDataSource(hikariConfig);
    }

    protected void createTable() throws SQLException {
        // Since we want to be Java 11 source compatible
        // Doc: https://clickhouse.com/docs/en/engines/table-engines
        String newLine = System.getProperty("line.separator");
        String createStatementTextBlock =
                "CREATE TABLE test.my_table"
                        + newLine
                        + "("
                        + newLine
                        + "`myfloat_nullable` Nullable(Float32),"
                        + newLine
                        + "`mystr` String,"
                        + newLine
                        + "`myint_id` Int32"
                        + newLine
                        + ") ENGINE = Log";

        LOGGER.info(createStatementTextBlock);

        LOGGER.info("About to execute CREATE TABLE statement: {}", createStatementTextBlock);
        Integer resultCreate = performStatement(clickhouseDBContainer, createStatementTextBlock);
        LOGGER.info("Successfully executed CREATE TABLE statement: {}", resultCreate);
    }

    protected void dropTable() throws SQLException {
        String dropStatementTextBlock =
                "DROP TABLE test.my_table";

        LOGGER.info(dropStatementTextBlock);

        LOGGER.info("About execute DROP TABLE statement: {}", dropStatementTextBlock);
        Integer resultCreate = performStatement(clickhouseDBContainer, dropStatementTextBlock);
        LOGGER.info("Successfully executed DROP TABLE statement: {}", resultCreate);
    }

    private static void browserClient(Integer port) throws IOException, InterruptedException {
        // We need to wait until the GUI is ready
        // If not ready: Hit "Reload structure" in Browser UI to see the table
        Thread.sleep(5000);
        String os = System.getProperty("os.name").toLowerCase();
        String tabuxURL = String.format("http://localhost:%s", port);
        if (os.equals("mac os x")) {
            Runtime.getRuntime().exec("open " + tabuxURL);
        } else if (os.equals("windows 10")) {
            Runtime.getRuntime().exec(String.format("cmd /c start %s", tabuxURL));
        } else {
            LOGGER.info("Please open a browser at: {}", tabuxURL);
        }
    }
}