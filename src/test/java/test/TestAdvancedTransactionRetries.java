package test;

import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.transaction.SerializableTransactionRunner;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class TestAdvancedTransactionRetries
{
    private static final int MAX_RETRIES = 5;
    private static final String SAVEPOINT_NAME = "cockroach_restart";
    private static final String SQLSTATE_TXN_SERIALIZATION_FAILED = "40001";

    public interface TestDao
    {
        @SqlUpdate("CREATE TABLE test (id int NOT NULL PRIMARY KEY, name STRING NOT NULL)")
        void createTable();

        @SqlUpdate("INSERT INTO test (id, name) VALUES (:id, :name)")
        void insert(@Bind("id") int id, @Bind("name") String name);

        @SqlQuery("SELECT name FROM test WHERE id = :id")
        Optional<String> get(@Bind("id") int id);
    }

    @Test
    public void testAdvancedTransactionRetries()
            throws Exception
    {
        ExtendedCockroachContainer container = new ExtendedCockroachContainer();
        container.start();

        Jdbi jdbi = Jdbi.create(() -> container.createConnection(""))
                .setTransactionHandler(new SerializableTransactionRunner())
                .installPlugin(new SqlObjectPlugin())
                .configure(SerializableTransactionRunner.Configuration.class, conf -> conf.setMaxRetries(0));

        jdbi.useExtension(TestDao.class, TestDao::createTable);

        doTest(jdbi, 1, "one", false);
        doTest(jdbi, 2, "two", true); // <--- this fails
    }

    private void doTest(Jdbi jdbi, int id, String name, boolean withSelect)
            throws Exception
    {
        jdbi.useHandle(handle -> {
            handle.execute("SET inject_retry_errors_enabled = 'true'");

            int attempts = retry(handle, () -> {
                if (withSelect) {
                    handle.attach(TestDao.class).get(id);
                }
                handle.attach(TestDao.class).insert(id, name);
            });
            assertThat(attempts).isGreaterThan(0);
        });
    }

    private static int retry(Handle handle, Runnable proc)
            throws Exception
    {
        handle.begin();
        Savepoint savepoint = handle.getConnection().setSavepoint(SAVEPOINT_NAME);

        for (int attempt = 0; attempt < MAX_RETRIES; ++attempt) {
            try {
                proc.run();
                handle.commit();
                return attempt;
            }
            catch (Exception e) {
                if (isRetryError(e)) {
                    handle.getConnection().rollback(savepoint);
                }
                else {
                    throw e;
                }
            }
        }

        throw new RuntimeException("Retries expired");
    }

    private static boolean isRetryError(Throwable throwable)
    {
        Throwable t = throwable;

        do {
            if (t instanceof SQLException) {
                String sqlState = ((SQLException) t).getSQLState();

                if (sqlState != null && sqlState.startsWith(SQLSTATE_TXN_SERIALIZATION_FAILED)) {
                    return true;
                }
            }
        }
        while ((t = t.getCause()) != null);

        return false;
    }
}
