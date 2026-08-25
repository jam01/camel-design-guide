package sandbox;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.camel.builder.RouteBuilder;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What limits how many transacted routes can run at once, and what it looks like when that limit
 * is reached.
 * <p>
 * A transaction boundary buffers nothing, so it has no queue of its own. Its bound is the
 * connection pool — and the boundary takes a connection when it opens, not when the first
 * statement runs, so it is held for every step of the route body including the ones that never
 * touch the database.
 */
public class TransactedBackpressureProbe extends ProbeSupport {

    private static final int POOL = 2;

    private final AtomicInteger inFlight = new AtomicInteger();
    private final AtomicInteger peak = new AtomicInteger();
    private final CountDownLatch bothIn = new CountDownLatch(POOL);
    private final CountDownLatch release = new CountDownLatch(1);

    @Override
    protected DataSource createDataSource() {
        var config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl());
        config.setUsername("sa");
        config.setMaximumPoolSize(POOL);
        config.setConnectionTimeout(1000);   // fail fast rather than hang the probe
        return new HikariDataSource(config);
    }

    @Override
    protected void doPostTearDown() throws Exception {
        release.countDown();
        // This probe's whole subject is connection exhaustion; leaking pools would be a poor
        // invariant to leave behind.
        if (dataSource instanceof HikariDataSource pool) {
            pool.close();
        }
    }

    @Override
    protected RouteBuilder createRouteBuilder() {
        return new RouteBuilder() {
            @Override
            public void configure() {
                // Note what this route does NOT do: any SQL at all.
                from("direct:transacted-no-sql")
                        .transacted()
                        .process(ex -> {
                            peak.accumulateAndGet(inFlight.incrementAndGet(), Math::max);
                            bothIn.countDown();
                            release.await(15, TimeUnit.SECONDS);
                            inFlight.decrementAndGet();
                        });

                from("direct:not-transacted")
                        .process(ex -> {
                            peak.accumulateAndGet(inFlight.incrementAndGet(), Math::max);
                            bothIn.countDown();
                            release.await(15, TimeUnit.SECONDS);
                            inFlight.decrementAndGet();
                        });
            }
        };
    }

    private Thread fire(String uri, AtomicInteger failures) {
        var t = new Thread(() -> {
            try {
                template.sendBody(uri, "in");
            } catch (Exception e) {
                failures.incrementAndGet();
            }
        });
        t.start();
        return t;
    }

    @Test
    void aTransactedRouteHoldsAConnectionForItsWholeBody_evenWithoutSql() throws Exception {
        var failures = new AtomicInteger();
        var threads = new Thread[POOL + 2];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = fire("direct:transacted-no-sql", failures);
        }

        assertThat(bothIn.await(5, TimeUnit.SECONDS)).isTrue();
        Thread.sleep(2000);   // longer than the pool's connection timeout

        assertThat(peak.get())
                .describedAs("the route issues no SQL, yet only as many ran as there are "
                        + "connections — the boundary takes one when it opens and keeps it for "
                        + "every step, so a slow call inside a transacted route is holding a "
                        + "database connection while it waits")
                .isEqualTo(POOL);
        assertThat(failures.get())
                .describedAs("and the excess did not queue politely: they waited on the pool and "
                        + "then failed with its timeout. The backpressure of a transaction boundary "
                        + "is the connection pool, and it arrives as an exception from the pool "
                        + "rather than anything Camel says.")
                .isEqualTo(2);

        release.countDown();
        for (var t : threads) {
            t.join(5000);
        }
    }

    @Test
    void anUntransactedRouteHasNoSuchLimit() throws Exception {
        var failures = new AtomicInteger();
        var threads = new Thread[POOL + 2];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = fire("direct:not-transacted", failures);
        }

        assertThat(bothIn.await(5, TimeUnit.SECONDS)).isTrue();
        Thread.sleep(1500);

        assertThat(peak.get())
                .describedAs("the same body without .transacted() takes no connection, so nothing "
                        + "caps it — the limit belongs to the boundary, not to the work")
                .isEqualTo(POOL + 2);
        assertThat(failures.get()).isZero();

        release.countDown();
        for (var t : threads) {
            t.join(5000);
        }
    }
}
