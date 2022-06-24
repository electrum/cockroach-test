package test;

import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.transaction.SerializableTransactionRunner;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntConsumer;

import static io.airlift.concurrent.Threads.threadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class TestCockroachTransaction
{
    @Test
    public void testTransaction()
            throws Exception
    {
        ExtendedCockroachContainer container = new ExtendedCockroachContainer();
        container.start();

        Jdbi jdbi = Jdbi.create(() -> container.createConnection(""))
                .setTransactionHandler(new SerializableTransactionRunner())
                .configure(SerializableTransactionRunner.Configuration.class, conf -> conf.setMaxRetries(0));

        String accountId = "a-12345678";
        String entityId = "c-12345678";

        jdbi.useHandle(handle -> {
            handle.execute("""
                    CREATE TABLE accounts (
                      account_id string PRIMARY KEY
                    ) LOCALITY GLOBAL
                    """);
            handle.execute("""
                    CREATE TABLE entities (
                      account_id string NOT NULL REFERENCES accounts,
                      entity_id string NOT NULL,
                      entity_kind string NOT NULL,
                      schema_name string,
                      table_name string,
                      column_name string,
                      schema_table_column string NOT NULL AS (coalesce(schema_name, '') || '/' || coalesce(table_name, '') || '/' || coalesce(column_name, '')) STORED,
                      PRIMARY KEY (account_id, entity_id, entity_kind, schema_table_column)
                    ) LOCALITY GLOBAL
                    """);
            handle.execute("INSERT INTO accounts VALUES (?)", accountId);
        });

        IntConsumer consumer = i -> {
            String schema = "test_schema";
            String table = "table_%04d".formatted(i);
            long start = System.nanoTime();
            jdbi.useTransaction(handle -> {
                handle.execute("""
                        DELETE FROM entities
                        WHERE account_id = ?
                          AND entity_id = ?
                          AND entity_kind = ?
                          AND schema_name = ?
                          AND table_name = ?
                        """, accountId, entityId, "TABLE", schema, table);
                handle.execute("""
                        INSERT INTO entities (account_id, entity_id, entity_kind, schema_name, table_name)
                        VALUES (?, ?, ?, ?, ?)
                        """, accountId, entityId, "TABLE", "test_schema", table);
            });
            System.out.printf("%s CREATED %s in %.3fs%n", Thread.currentThread().getName(), table, Duration.ofNanos(System.nanoTime() - start).toMillis() / 1000.0);
        };

        ExecutorService executor = newCachedThreadPool(threadsNamed("test-%02d"));
        AtomicInteger index = new AtomicInteger();
        List<Future<?>> futures = new ArrayList<>();
        for (int thread = 0; thread < 20; thread++) {
            futures.add(executor.submit(() -> {
                while (true) {
                    int i = index.getAndIncrement();
                    if (i >= 1000) {
                        return;
                    }
                    consumer.accept(i);
                }
            }));
        }
        executor.shutdown();
        for (Future<?> future : futures) {
            future.get();
        }
    }
}
