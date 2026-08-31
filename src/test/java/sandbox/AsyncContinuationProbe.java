package sandbox;

import org.apache.camel.AsyncCallback;
import org.apache.camel.AsyncProcessor;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.support.AsyncProcessorSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Whether a non-blocking producer really releases the thread it was called on, and whether that
 * survives being wrapped in a {@code .circuitBreaker()}.
 * <p>
 * The advice not to hop before an async producer rests on the first half: the producer dispatches,
 * returns {@code false}, and whatever runs next runs on the thread that received the response. That
 * is also the premise behind sizing a bulkhead by the dependency's latency rather than by thread
 * count — permits and threads are only independent if no thread is held while a permit is.
 * <p>
 * The stand-in for a non-blocking component here is an {@link AsyncProcessor} that returns
 * {@code false} and completes later on a thread of its own. That is the whole contract a component
 * has to honour to be non-blocking, so the measurement is about Camel's routing engine rather than
 * about any one component.
 */
public class AsyncContinuationProbe extends ProbeSupport {

    private final List<String> threads = new CopyOnWriteArrayList<>();
    private final ExecutorService completer = Executors.newSingleThreadExecutor(r -> new Thread(r, "completer"));

    private CountDownLatch dispatched = new CountDownLatch(1);
    private CountDownLatch release = new CountDownLatch(1);

    private void mark(String label) {
        threads.add(label + "=" + Thread.currentThread().getName());
    }

    private String threadOf(String label) {
        return threads.stream().filter(t -> t.startsWith(label + "="))
                .map(t -> t.substring(label.length() + 1)).findFirst().orElse(null);
    }

    /**
     * Dispatches and returns without a result, exactly as a non-blocking producer does: the caller
     * is told routing continues elsewhere, and the response lands on another thread later.
     */
    private final class Dispatch extends AsyncProcessorSupport {
        @Override
        public boolean process(Exchange exchange, AsyncCallback callback) {
            mark("dispatch");
            completer.submit(() -> {
                try {
                    release.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                callback.done(false);
            });
            dispatched.countDown();
            return false;                 // "I am not done; do not hold a thread for me"
        }
    }

    @Override
    protected RouteBuilder createRouteBuilder() {
        return new RouteBuilder() {
            @Override
            public void configure() {
                from("direct:plain")
                        .process(ex -> mark("before"))
                        .process(new Dispatch())
                        .process(ex -> mark("after"));

                from("direct:breaker")
                        .process(ex -> mark("cb-before"))
                        .circuitBreaker()
                            .process(new Dispatch())
                        .end()
                        .process(ex -> mark("cb-after"));
            }
        };
    }

    @AfterEach
    void stopCompleter() {
        release.countDown();
        completer.shutdownNow();
    }

    /** Runs the send on a thread we can name, so "the caller's thread" is identifiable. */
    private Thread send(String uri) throws Exception {
        var caller = new Thread(() -> template.request(uri, ex -> ex.getIn().setBody("in")), "probe-caller");
        caller.start();
        assertThat(dispatched.await(5, TimeUnit.SECONDS)).isTrue();
        return caller;
    }

    @Test
    void anAsyncProducerReleasesTheCallersThread_andTheRestOfTheRouteRunsOnTheResponseThread() throws Exception {
        var caller = send("direct:plain");

        release.countDown();
        caller.join(5000);

        assertThat(threadOf("before")).isEqualTo("probe-caller");
        assertThat(threadOf("dispatch")).isEqualTo("probe-caller");
        assertThat(threadOf("after"))
                .describedAs("the step after the async call runs on the thread that received the "
                        + "response, not the one that made the call — so there is nothing to hop "
                        + "away from, and everything downstream inherits the response thread")
                .isEqualTo("completer");
    }

    @Test
    void insideABreakerTheSameProducerHoldsTheCallersThreadForTheWholeCall() throws Exception {
        var caller = send("direct:breaker");

        release.countDown();
        caller.join(5000);

        assertThat(threadOf("cb-before")).isEqualTo("probe-caller");
        assertThat(threadOf("dispatch")).isEqualTo("probe-caller");
        assertThat(threadOf("cb-after"))
                .describedAs("the step after end() runs on the thread that made the call, not on "
                        + "the one that received the response — so that thread was inside the "
                        + "breaker, parked, for the whole call. ResilienceProcessor runs the block "
                        + "through the synchronous Processor.process(copy) and awaits it, so a "
                        + "permit and a thread are occupied over the same interval however "
                        + "non-blocking the component inside the block is")
                .isEqualTo("probe-caller");
    }
}
