package sandbox;

import org.apache.camel.builder.RouteBuilder;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What a {@code .circuitBreaker()} block is as a boundary: who owns a failure inside it, whose
 * thread the call runs on, and what a full bulkhead does to the caller.
 * <p>
 * It is the one cut you place for resilience rather than for structure, so it is worth knowing
 * which of the properties the other cuts have it also has.
 */
public class CircuitBreakerProbe extends ProbeSupport {

    private final List<String> events = new CopyOnWriteArrayList<>();
    private final List<String> threads = new CopyOnWriteArrayList<>();
    private final CountDownLatch permitHeld = new CountDownLatch(1);
    private final CountDownLatch releasePermit = new CountDownLatch(1);

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
                onException(Boom.class).handled(true).setBody(constant("CLAUSE"));

                from("direct:cb-fallback")
                        .circuitBreaker()
                            .process(ex -> {
                                throw new Boom();
                            })
                        .onFallback()
                            .setBody(constant("FALLBACK"))
                        .end()
                        .process(ex -> events.add("carried-on"));

                from("direct:cb-thread")
                        .process(ex -> mark("before"))
                        .circuitBreaker()
                            .process(ex -> mark("plain"))
                        .end();

                from("direct:cb-timeout")
                        .process(ex -> mark("t-before"))
                        .circuitBreaker()
                            .resilience4jConfiguration()
                                .timeoutEnabled(true)
                                .timeoutDuration(5000)
                            .end()
                            .process(ex -> mark("timed"))
                        .end();

                // Camel suppresses .threads() inside a transaction. Resilience4j's time limiter
                // is not Camel's ThreadsProcessor, so nothing suppresses it.
                from("direct:cb-tx")
                        .transacted()
                        .to(insertRow("before"))
                        .circuitBreaker()
                            .resilience4jConfiguration()
                                .timeoutEnabled(true)
                                .timeoutDuration(5000)
                            .end()
                            .process(ex -> mark("tx-inside"))
                            .to(insertRow("inside"))
                        .end()
                        .process(ex -> {
                            throw new OtherBoom();
                        });

                // A fallback clears the exception. Inside a transacted route, that is the same
                // shape as handled(true) — the boundary is handed a clean exchange.
                from("direct:tx-with-fallback")
                        .transacted()
                        .to(insertRow("before"))
                        .circuitBreaker()
                            .process(ex -> {
                                throw new OtherBoom();
                            })
                        .onFallback()
                            .setBody(constant("FALLBACK"))
                        .end()
                        .to(insertRow("after"));

                // And doCatch clears it too — the same shape again, a third way in.
                from("direct:tx-with-catch")
                        .transacted()
                        .to(insertRow("before"))
                        .doTry()
                            .process(ex -> {
                                throw new OtherBoom();
                            })
                        .doCatch(OtherBoom.class)
                            .setBody(constant("caught"))
                        .end()
                        .to(insertRow("after"));

                from("direct:cb-bulkhead")
                        .circuitBreaker()
                            .resilience4jConfiguration()
                                .bulkheadEnabled(true)
                                .bulkheadMaxConcurrentCalls(1)
                            .end()
                            .process(ex -> {
                                permitHeld.countDown();
                                releasePermit.await(10, TimeUnit.SECONDS);
                            })
                        .onFallback()
                            .setBody(constant("REJECTED"))
                        .end();
            }
        };
    }

    @Test
    void theFallbackOwnsTheFailure_andTheRouteCarriesOnAfterEnd() {
        var out = template.request("direct:cb-fallback", ex -> ex.getIn().setBody("in"));

        assertThat(out.getMessage().getBody(String.class))
                .describedAs("the block handled it itself — the builder's clause never saw it, so a "
                        + "circuit breaker claims like a callee that owns its errors")
                .isEqualTo("FALLBACK");
        assertThat(events)
                .describedAs("...but unlike a claimed failure, routing resumes after end(), the way "
                        + "doCatch does")
                .containsExactly("carried-on");
        assertThat(out.getException()).isNull();
    }

    @Test
    void byDefaultTheCallRunsOnTheCallersThread() {
        template.request("direct:cb-thread", ex -> ex.getIn().setBody("in"));

        assertThat(threadOf("plain"))
                .describedAs("no time limiter, no hop — the breaker is a semaphore, not a pool")
                .isEqualTo(threadOf("before"));
    }

    @Test
    void enablingTheTimeoutReintroducesAThreadHop() {
        template.request("direct:cb-timeout", ex -> ex.getIn().setBody("in"));

        assertThat(threadOf("timed"))
                .describedAs("timeoutEnabled brings Resilience4j's TimeLimiter, which has to run "
                        + "the call on another thread to be able to abandon it — so a config flag "
                        + "that reads like a timeout is also a thread boundary")
                .isNotEqualTo(threadOf("t-before"));
    }

    @Test
    void aTimeLimiterInsideATransaction_movesTheWorkOutOfIt() throws Exception {
        template.request("direct:cb-tx", ex -> ex.getIn().setBody("in"));

        assertThat(threadOf("tx-inside"))
                .describedAs("Camel refuses to hop a transacted exchange, but this hop is "
                        + "Resilience4j's, and nothing tells it not to")
                .isNotEqualTo(Thread.currentThread().getName());
        assertThat(notes())
                .describedAs("the surviving row is the one written INSIDE the breaker, on another "
                        + "thread and therefore outside the transaction; the write before it "
                        + "rolled back. A timeout flag silently split the route's atomicity in "
                        + "two, and a bare count could not tell which half survived.")
                .containsExactly("inside");
    }

    @Test
    void aFullBulkheadRejectsImmediatelyIntoTheFallback() throws Exception {
        var holder = new Thread(() -> template.request("direct:cb-bulkhead", e -> e.getIn().setBody("hold")));
        holder.start();
        assertThat(permitHeld.await(5, TimeUnit.SECONDS)).isTrue();

        var start = System.nanoTime();
        var out = template.request("direct:cb-bulkhead", ex -> ex.getIn().setBody("in"));
        var waitedMs = (System.nanoTime() - start) / 1_000_000;

        assertThat(out.getMessage().getBody(String.class))
                .describedAs("the permit was taken, so the second call never ran and the fallback "
                        + "answered for it — rejection and failure arrive at the same place")
                .isEqualTo("REJECTED");
        assertThat(waitedMs)
                .describedAs("and it did not queue: bulkheadMaxWaitDuration defaults to 0, so a "
                        + "full bulkhead sheds load rather than absorbing it")
                .isLessThan(1000);

        releasePermit.countDown();
        holder.join(5000);
    }

    @Test
    void aFallbackInsideATransactedRouteCommitsThePartialWork() throws Exception {
        var out = template.request("direct:tx-with-fallback", ex -> ex.getIn().setBody("in"));

        assertThat(out.getMessage().getBody(String.class)).isEqualTo("FALLBACK");
        assertThat(notes())
                .describedAs("the breaker's fallback cleared the exception, so the route carried on "
                        + "and the boundary was handed a clean exchange — the write made before the "
                        + "failure COMMITTED. onFallback is handled(true) wearing a different name, "
                        + "and it needs markRollbackOnly() for the same reason.")
                .containsExactly("before", "after");
    }

    @Test
    void andSoDoesADoCatch_becauseItIsTheSameClearingStep() throws Exception {
        template.request("direct:tx-with-catch", ex -> ex.getIn().setBody("in"));

        assertThat(notes())
                .describedAs("a third construct with the same consequence: doCatch clears the "
                        + "exception, so the boundary commits the work done before the failure. "
                        + "The rule is not about handled(true) — it is about anything that clears "
                        + "the exception before the boundary looks.")
                .containsExactly("before", "after");
    }
}
