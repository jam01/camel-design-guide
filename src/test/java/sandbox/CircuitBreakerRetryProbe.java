package sandbox;

import org.apache.camel.builder.RouteBuilder;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What retry means around a circuit breaker, given that the breaker is counting.
 * <p>
 * Camel's redelivery and Resilience4j's failure statistics know nothing about each other, and they
 * are stacked one inside the other. That makes the interesting question not "does retry work" but
 * "what does a retry do to the thing deciding whether to stop calling at all".
 */
public class CircuitBreakerRetryProbe extends ProbeSupport {

    private final AtomicInteger escaping = new AtomicInteger();
    private final AtomicInteger falling = new AtomicInteger();
    private final AtomicInteger opening = new AtomicInteger();
    private final AtomicInteger control = new AtomicInteger();

    @Override
    protected RouteBuilder createRouteBuilder() {
        return new RouteBuilder() {
            @Override
            public void configure() {
                errorHandler(defaultErrorHandler().maximumRedeliveries(2).redeliveryDelay(0));

                // Control: the same failure with no breaker around it.
                from("direct:control")
                        .process(ex -> {
                            control.incrementAndGet();
                            throw new Boom();
                        });

                // No fallback: the failure escapes the breaker and meets the error handler.
                from("direct:escapes")
                        .circuitBreaker()
                            .process(ex -> {
                                escaping.incrementAndGet();
                                throw new Boom();
                            })
                        .end();

                // A fallback answers, so nothing escapes.
                from("direct:falls-back")
                        .circuitBreaker()
                            .process(ex -> {
                                falling.incrementAndGet();
                                throw new Boom();
                            })
                        .onFallback()
                            .setBody(constant("FALLBACK"))
                        .end();

                // Small enough window to open inside a test.
                from("direct:opens")
                        .circuitBreaker()
                            .resilience4jConfiguration()
                                .slidingWindowSize(4)
                                .minimumNumberOfCalls(4)
                                .failureRateThreshold(50)
                                .waitDurationInOpenState(30000)
                            .end()
                            .process(ex -> {
                                opening.incrementAndGet();
                                throw new Boom();
                            })
                        .onFallback()
                            .setBody(constant("FALLBACK"))
                        .end();
            }
        };
    }

    @Test
    void controlShowsTheErrorHandlerIsRetrying() {
        template.request("direct:control", ex -> ex.getIn().setBody("in"));
        assertThat(control.get()).isEqualTo(3);
    }

    @Test
    void redeliveryDoesNotReEnterTheBreaker_soRetryQuietlyStopsAtItsEdge() {
        template.request("direct:escapes", ex -> ex.getIn().setBody("in"));

        assertThat(escaping.get())
                .describedAs("the identical failure under the identical handler was retried three "
                        + "times in the control route and once here. A circuit breaker sets the "
                        + "exception on the exchange and returns rather than throwing through the "
                        + "channel, and redelivery never re-enters it — so maximumRedeliveries "
                        + "applies everywhere in the route except inside this block, and says so "
                        + "nowhere.")
                .isEqualTo(1);
    }

    @Test
    void aFallbackStopsRetryBeforeItStarts() {
        var out = template.request("direct:falls-back", ex -> ex.getIn().setBody("in"));

        assertThat(out.getMessage().getBody(String.class)).isEqualTo("FALLBACK");
        assertThat(falling.get())
                .describedAs("the fallback cleared the exception, so the error handler saw a "
                        + "success and never retried — adding onFallback silently turns retry off, "
                        + "the same way giving a clause a body does")
                .isEqualTo(1);
    }

    @Test
    void onceOpen_theBodyIsNotCalledAtAll_andTheFallbackStillAnswers() {
        var bodies = new ArrayList<String>();
        for (int i = 0; i < 10; i++) {
            bodies.add(template.request("direct:opens", ex -> ex.getIn().setBody("in"))
                    .getMessage().getBody(String.class));
        }

        assertThat(opening.get())
                .describedAs("four calls filled the window and tripped the breaker; the remaining "
                        + "six never reached the body at all")
                .isEqualTo(4);
        assertThat(bodies)
                .describedAs("and every caller still got an answer — an open circuit is not an "
                        + "error path, it is the fallback path arriving faster")
                .hasSize(10)
                .allMatch("FALLBACK"::equals);
    }
}
