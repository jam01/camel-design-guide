package sandbox;

import org.apache.camel.CamelExecutionException;
import org.apache.camel.builder.RouteBuilder;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What happens at a thread boundary when the consumer is slower than the producer.
 * <p>
 * Every in-memory handoff has a bounded queue, and the only real question is what the producer is
 * told when that queue is full: an exception, a block, or nothing at all. All three are one option
 * apart, and the default is not the same for a queue endpoint as it is for a thread pool.
 */
public class BackpressureProbe extends ProbeSupport {

    private static final String STRICT = "seda:strict?size=1&pollTimeout=100";
    private static final String BLOCKING = "seda:blocking?size=1&blockWhenFull=true&pollTimeout=100";
    private static final String DISCARDING = "seda:discarding?size=1&discardWhenFull=true&pollTimeout=100";

    private final CountDownLatch busy = new CountDownLatch(1);
    private final CountDownLatch release = new CountDownLatch(1);
    private final List<String> consumed = new CopyOnWriteArrayList<>();
    private final List<String> timedOut = new CopyOnWriteArrayList<>();
    private final List<String> ranOn = new CopyOnWriteArrayList<>();

    private final CountDownLatch poolBusy = new CountDownLatch(1);
    private final CountDownLatch poolRelease = new CountDownLatch(1);

    /** Occupies the single consumer until the test lets go. */
    private void occupy(String body) throws Exception {
        busy.countDown();
        if (!release.await(10, TimeUnit.SECONDS)) {
            timedOut.add(body);      // a hold that expired would silently change every count below
        }
        consumed.add(body);
    }

    @Override
    protected RouteBuilder createRouteBuilder() {
        return new RouteBuilder() {
            @Override
            public void configure() {
                from(STRICT).process(ex -> occupy(ex.getIn().getBody(String.class)));
                from(BLOCKING).process(ex -> occupy(ex.getIn().getBody(String.class)));
                from(DISCARDING).process(ex -> occupy(ex.getIn().getBody(String.class)));

                // One thread, no queue at all, so the second concurrent task must be rejected.
                from("direct:pooled")
                        .threads().poolSize(1).maxPoolSize(1).maxQueueSize(0)
                        .process(ex -> {
                            var tag = ex.getIn().getHeader("tag", String.class);
                            ranOn.add(tag + "=" + Thread.currentThread().getName());
                            if ("A".equals(tag)) {
                                poolBusy.countDown();
                                poolRelease.await(10, TimeUnit.SECONDS);
                            }
                        });
            }
        };
    }

    private void fillTo(String uri) throws Exception {
        template.sendBody(uri, "1");                       // taken by the consumer, which blocks
        assertThat(busy.await(5, TimeUnit.SECONDS)).isTrue();
        template.sendBody(uri, "2");                       // fills the queue of size 1
    }

    @Test
    void aFullQueueThrowsAtTheProducerByDefault() throws Exception {
        fillTo(STRICT);

        assertThatThrownBy(() -> template.sendBody(STRICT, "3"))
                .describedAs("the default is neither blocking nor silent — the send fails, and the "
                        + "backpressure arrives at the producer as an exception it must handle")
                .isInstanceOf(CamelExecutionException.class)
                .hasRootCauseInstanceOf(IllegalStateException.class);

        release.countDown();
    }

    @Test
    void blockWhenFull_turnsTheProducerIntoTheBrake() throws Exception {
        fillTo(BLOCKING);

        var sender = new Thread(() -> template.sendBody(BLOCKING, "3"));
        sender.start();

        sender.join(500);
        assertThat(sender.isAlive())
                .describedAs("the producer is parked inside the send, waiting for room — which is "
                        + "backpressure, and also a thread held by a queue with no timeout")
                .isTrue();

        release.countDown();
        sender.join(5000);
        assertThat(sender.isAlive()).describedAs("and it completes once space appears").isFalse();
    }

    @Test
    void discardWhenFull_losesTheMessageAndTellsNobody() throws Exception {
        fillTo(DISCARDING);

        template.sendBody(DISCARDING, "3");   // no exception, no block
        release.countDown();

        for (int i = 0; i < 50 && consumed.size() < 2; i++) {
            Thread.sleep(20);
        }
        assertThat(timedOut).describedAs("no hold expired, so the counts mean what they say").isEmpty();
        assertThat(consumed)
                .describedAs("the third message was dropped on the floor with the producer none "
                        + "the wiser — the one option that turns overload into silent data loss")
                .containsExactly("1", "2");
    }

    @Test
    void aRejectedPoolTaskRunsOnTheCallersThread_becauseTheDefaultIsCallerRuns() throws Exception {
        var caller = Thread.currentThread().getName();

        var first = new Thread(() ->
                template.sendBodyAndHeader("direct:pooled", "x", "tag", "A"));
        first.start();
        assertThat(poolBusy.await(5, TimeUnit.SECONDS)).isTrue();

        template.sendBodyAndHeader("direct:pooled", "x", "tag", "B");

        assertThat(ranOn)
                .describedAs("the pool's only thread was busy and there is no queue, so B was "
                        + "rejected — and CallerRuns, Camel's default, executed it on the thread "
                        + "that submitted it. The hop silently did not happen.")
                .contains("B=" + caller);

        poolRelease.countDown();
        first.join(5000);
    }
}
