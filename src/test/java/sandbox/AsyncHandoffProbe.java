package sandbox;

import org.apache.camel.builder.RouteBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What happens to an in-memory handoff when the transaction that produced it does not commit.
 * <p>
 * Handing work to another stage through a queue looks like it defers the work until the current
 * one is done. It does not: the send happens where it is written, inside the transaction, and the
 * receiving stage is already running while the sender is still deciding whether to commit. These
 * probes measure both halves of that — the trigger that survives a rollback, and the trigger that
 * arrives before the data it describes.
 */
public class AsyncHandoffProbe extends ProbeSupport {

    private final List<String> downstream = new CopyOnWriteArrayList<>();
    private final CountDownLatch observed = new CountDownLatch(1);
    private final CountDownLatch delivered = new CountDownLatch(1);

    @BeforeEach
    void resetOutbox() throws Exception {
        execute("DROP TABLE IF EXISTS outbox");
        execute("CREATE TABLE outbox (id INT AUTO_INCREMENT PRIMARY KEY, payload VARCHAR(64))");
    }

    private int outboxRows() throws Exception {
        try (var c = dataSource.getConnection();
             var s = c.createStatement();
             var rs = s.executeQuery("SELECT count(*) FROM outbox")) {
            rs.next();
            return rs.getInt(1);
        }
    }

    @Override
    protected RouteBuilder createRouteBuilder() {
        return new RouteBuilder() {
            @Override
            public void configure() {
                // waitForTaskToComplete=Never makes the send fire-and-forget even though the
                // caller's exchange is InOut; otherwise the producer blocks for a reply.
                var queue = "seda:downstream?waitForTaskToComplete=Never";

                from("direct:queue-then-fail")
                        .transacted()
                        .to(insertRow("row"))
                        .to(queue)
                        .process(ex -> {
                            throw new Boom();
                        });

                from("direct:outbox-then-fail")
                        .transacted()
                        .to(insertRow("row"))
                        .to("sql:INSERT INTO outbox (payload) VALUES ('trigger')?dataSource=#dataSource")
                        .process(ex -> {
                            throw new Boom();
                        });

                // Sends, then waits for the receiver to have looked, before committing.
                from("direct:queue-then-commit")
                        .transacted()
                        .to(insertRow("row"))
                        .to(queue)
                        .process(ex -> observed.await(5, TimeUnit.SECONDS));

                from("seda:downstream?pollTimeout=100")
                        .process(ex -> {
                            downstream.add("committedRows=" + rows());
                            observed.countDown();
                            delivered.countDown();
                        });
            }
        };
    }

    private void run(String uri) {
        template.request(uri, ex -> ex.getIn().setBody("in"));
    }

    @Test
    void aQueuedHandoff_isDeliveredEvenThoughTheTransactionRolledBack() throws Exception {
        run("direct:queue-then-fail");

        assertThat(delivered.await(5, TimeUnit.SECONDS))
                .describedAs("the send is not part of the transaction, so the next stage still ran")
                .isTrue();
        assertThat(rows())
                .describedAs("...for a row that does not exist — the downstream stage was triggered "
                        + "for work that never happened")
                .isZero();
    }

    @Test
    void anOutboxRowIsInTheTransaction_soTheTriggerDisappearsWithTheWork() throws Exception {
        run("direct:outbox-then-fail");

        assertThat(rows()).isZero();
        assertThat(outboxRows())
                .describedAs("writing the trigger to the same database as the work makes it one "
                        + "resource manager: the rollback takes both, and no relay ever sees it")
                .isZero();
    }

    @Test
    void aQueuedHandoff_canBeReadBeforeTheSenderHasCommitted() throws Exception {
        run("direct:queue-then-commit");

        assertThat(downstream)
                .describedAs("the receiver read through its own connection while the sender was "
                        + "still inside the transaction. The wait makes the ordering observable; "
                        + "in production this is a race the receiver loses under load, not a "
                        + "guarantee it never loses.")
                .containsExactly("committedRows=0");
        assertThat(rows()).describedAs("the sender did go on to commit").isEqualTo(1);
    }
}
