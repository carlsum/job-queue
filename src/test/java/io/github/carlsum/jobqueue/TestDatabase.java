package io.github.carlsum.jobqueue;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;
import javax.sql.DataSource;

/**
 * One real PostgreSQL server shared by every test in the run, started on first use with the
 * library's schema applied, behind a HikariCP connection pool. Its binaries and data live under
 * {@code target/}, so nothing is installed on the machine and {@code mvn clean} removes it all.
 */
final class TestDatabase {

    private static final String SCHEMA = "/db/jobqueue/V1__create_job_queue.sql";
    private static final DataSource DATA_SOURCE = start();

    private TestDatabase() {}

    static DataSource dataSource() {
        return DATA_SOURCE;
    }

    /** Deletes every job. */
    static void reset() {
        execute("TRUNCATE job_queue RESTART IDENTITY");
    }

    /** Runs an INSERT, UPDATE or DELETE and returns how many rows it changed. */
    static int execute(String sql, Object... params) {
        try (Connection connection = DATA_SOURCE.getConnection();
                PreparedStatement statement = prepare(connection, sql, params)) {
            return statement.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    /** Runs a query that returns a single number. */
    static long queryLong(String sql, Object... params) {
        try (Connection connection = DATA_SOURCE.getConnection();
                PreparedStatement statement = prepare(connection, sql, params);
                ResultSet row = statement.executeQuery()) {
            row.next();
            return row.getLong(1);
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    private static PreparedStatement prepare(Connection connection, String sql, Object... params)
            throws SQLException {
        PreparedStatement statement = connection.prepareStatement(sql);
        for (int i = 0; i < params.length; i++) {
            statement.setObject(i + 1, params[i]);
        }
        return statement;
    }

    private static DataSource start() {
        try {
            EmbeddedPostgres postgres = EmbeddedPostgres.builder()
                    .setOverrideWorkingDirectory(new File("target/embedded-postgres"))
                    .setDataDirectory(new File("target/embedded-postgres/data-" + UUID.randomUUID()))
                    .setCleanDataDirectory(true)
                    .start();
            HikariConfig pool = new HikariConfig();
            pool.setJdbcUrl(postgres.getJdbcUrl("postgres", "postgres"));
            pool.setUsername("postgres");
            pool.setMaximumPoolSize(20);
            HikariDataSource dataSource = new HikariDataSource(pool);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    dataSource.close();
                    postgres.close();
                } catch (IOException | RuntimeException e) {
                    System.err.println("Could not stop the test database: " + e);
                }
            }));
            try (Connection connection = dataSource.getConnection();
                    Statement statement = connection.createStatement()) {
                statement.execute(readSchema());
            }
            return dataSource;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    private static String readSchema() throws IOException {
        try (InputStream in = TestDatabase.class.getResourceAsStream(SCHEMA)) {
            if (in == null) {
                throw new IllegalStateException(SCHEMA + " is not on the classpath");
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
