package sandbox;

import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.spi.ThreadPoolProfile;
import org.apache.camel.util.concurrent.ThreadPoolRejectedPolicy;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What a full thread pool tells the route, and what the route can do about it.
 * <p>
 * A pool is only a bulkhead if its rejection is something you can answer with. The default is not:
 * {@code CallerRuns} silently keeps the work on the calling thread, so the bound you thought you
 * placed is not there. The alternative is a named profile, and the surprise is where the policy is
 * read from — naming a profile on {@code .threads()} carries the policy along with the pool.
 */
public class RejectionPolicyProbe extends ProbeSupport {

    private final List<String> ranOn = new CopyOnWriteArrayList<>();
    private final CountDownLatch busy = new CountDownLatch(1);
    private final CountDownLatch release = new CountDownLatch(1);

    @Override
    protected RouteBuilder createRouteBuilder() {
        return new RouteBuilder() {
            @Override
            public void configure() {
                // One thread, no queue: the second concurrent task has nowhere to go.
                var shedding = new ThreadPoolProfile("db-api");
                shedding.setPoolSize(1);
                shedding.setMaxPoolSize(1);
                shedding.setMaxQueueSize(0);
                shedding.setRejectedPolicy(ThreadPoolRejectedPolicy.Abort);
                getContext().getExecutorServiceManager().registerThreadPoolProfile(shedding);

                onException(RejectedExecutionException.class)
                        .handled(true)
                        .setHeader(Exchange.HTTP_RESPONSE_CODE, constant(429))
                        .setHeader("Retry-After", constant("1"))
                        .setBody(constant("SHED"));

                from("direct:shed")
                        .threads().executorService("db-api")
                        .process(ex -> {
                            ranOn.add(ex.getIn().getHeader("tag", String.class) + "="
                                    + Thread.currentThread().getName());
                            if ("A".equals(ex.getIn().getHeader("tag"))) {
                                busy.countDown();
                                release.await(10, TimeUnit.SECONDS);
                            }
                        });
            }
        };
    }

    @Test
    void thereIsNoBlockingRejectionPolicy() {
        assertThat(ThreadPoolRejectedPolicy.values())
                .describedAs("the enum offers two answers to a full pool, and parking the caller "
                        + "until space appears is not one of them — advice to configure a blocking "
                        + "policy has nothing to configure. A parked producer is available at a "
                        + "queue endpoint (seda blockWhenFull) but not at a thread pool")
                .containsExactly(ThreadPoolRejectedPolicy.Abort, ThreadPoolRejectedPolicy.CallerRuns);
    }

    @Test
    void anAbortProfileArrivesAsAMappableFailure_soTheRouteCanAnswer429() throws Exception {
        var holder = new Thread(() -> template.request("direct:shed",
                ex -> ex.getIn().setHeader("tag", "A")), "holder");
        holder.start();
        assertThat(busy.await(5, TimeUnit.SECONDS)).isTrue();

        var out = template.request("direct:shed", ex -> ex.getIn().setHeader("tag", "B"));

        assertThat(out.getMessage().getBody(String.class))
                .describedAs("the rejection is set on the exchange rather than thrown at the "
                        + "sender, so it reaches the route's own clause like any other failure — "
                        + "which is what makes a bounded pool something an HTTP edge can answer "
                        + "with a status instead of a stall")
                .isEqualTo("SHED");
        assertThat(out.getMessage().getHeader(Exchange.HTTP_RESPONSE_CODE)).isEqualTo(429);
        assertThat(out.getException()).isNull();

        assertThat(ranOn)
                .describedAs("and the rejected work did NOT run: naming the profile on .threads() "
                        + "carried its Abort policy across, where the default would have run it on "
                        + "the caller's thread and reported nothing")
                .noneMatch(entry -> entry.startsWith("B="));

        release.countDown();
        holder.join(5000);
    }
}
