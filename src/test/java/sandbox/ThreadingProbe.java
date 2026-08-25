package sandbox;

import org.apache.camel.builder.RouteBuilder;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Where the thread changes, and what does not follow it across.
 * <p>
 * A thread hop is the one boundary that is invisible in the DSL — {@code .threads()} and a queue
 * endpoint read like ordinary steps. What matters is which of the things a route is carrying make
 * the jump with it, and a transaction is the one that does not.
 */
public class ThreadingProbe extends ProbeSupport {

    private final List<String> threads = new CopyOnWriteArrayList<>();
    private final CountDownLatch tapped = new CountDownLatch(1);
    private final CountDownLatch consumed = new CountDownLatch(1);

    private void mark(String label) {
        threads.add(label + "=" + Thread.currentThread().getName());
    }

    private String threadOf(String label) {
        return threads.stream().filter(t -> t.startsWith(label + "="))
                .map(t -> t.substring(label.length() + 1)).findFirst().orElse(null);
    }

    @Override
    protected RouteBuilder createRouteBuilder() {
        return new RouteBuilder() {
            @Override
            public void configure() {
                from("direct:same-thread")
                        .process(ex -> mark("caller"))
                        .to("direct:same-thread-callee");

                from("direct:same-thread-callee")
                        .process(ex -> mark("callee"));

                from("direct:hop")
                        .process(ex -> mark("before-hop"))
                        .threads(1)
                        .process(ex -> mark("after-hop"));

                // The question that matters: does the transaction cross the hop?
                from("direct:tx-hop")
                        .transacted()
                        .to(insertRow("before-hop"))
                        .process(ex -> mark("tx-before-hop"))
                        .threads(1)
                        .process(ex -> mark("tx-after-hop"))
                        .to(insertRow("after-hop"))
                        .process(ex -> {
                            throw new Boom();
                        });

                from("direct:tap")
                        .wireTap("direct:tapped")
                        .process(ex -> mark("tap-origin"));

                from("direct:tapped")
                        .process(ex -> {
                            mark("tapped");
                            tapped.countDown();
                        });

                from("direct:to-queue")
                        .to("seda:work?waitForTaskToComplete=Never")
                        .process(ex -> mark("sender"));

                from("seda:work?pollTimeout=100")
                        .process(ex -> {
                            mark("consumer");
                            consumed.countDown();
                        });
            }
        };
    }

    @Test
    void aDirectCallStaysOnTheCallersThread() {
        template.request("direct:same-thread", ex -> ex.getIn().setBody("in"));

        assertThat(threadOf("callee"))
                .describedAs("a direct: hop is a method call — same thread, so anything thread-bound "
                        + "survives it")
                .isEqualTo(threadOf("caller"));
    }

    @Test
    void threadsChangesTheThreadMidRoute() {
        template.request("direct:hop", ex -> ex.getIn().setBody("in"));

        assertThat(threadOf("after-hop"))
                .describedAs(".threads() looks like an ordinary step and is a boundary")
                .isNotEqualTo(threadOf("before-hop"));
    }

    @Test
    void insideATransaction_threadsIsSilentlyIgnored() throws Exception {
        template.request("direct:tx-hop", ex -> ex.getIn().setBody("in"));

        assertThat(threadOf("tx-after-hop"))
                .describedAs("ThreadsProcessor returns immediately when the exchange is transacted, "
                        + "because a transaction manager cannot span threads. The hop does not "
                        + "happen at all — the concurrency you asked for is not there.")
                .isEqualTo(threadOf("tx-before-hop"));
        assertThat(rows())
                .describedAs("...which is why atomicity survives: both writes stayed on the one "
                        + "thread, in the one transaction, and both rolled back")
                .isZero();
    }

    @Test
    void aWireTapRunsOnItsOwnThread() throws Exception {
        template.request("direct:tap", ex -> ex.getIn().setBody("in"));
        assertThat(tapped.await(5, TimeUnit.SECONDS)).isTrue();

        assertThat(threadOf("tapped"))
                .describedAs("a wireTap is a thread boundary as well as a copy")
                .isNotEqualTo(threadOf("tap-origin"));
    }

    @Test
    void aQueueEndpointMovesTheWorkToAConsumerThread() throws Exception {
        template.request("direct:to-queue", ex -> ex.getIn().setBody("in"));
        assertThat(consumed.await(5, TimeUnit.SECONDS)).isTrue();

        assertThat(threadOf("consumer")).isNotEqualTo(threadOf("sender"));
    }
}
