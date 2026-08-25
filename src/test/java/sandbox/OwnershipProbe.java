package sandbox;

import org.apache.camel.builder.RouteBuilder;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Who gets to decide what a failure becomes, when a route in one builder calls a route in another.
 * <p>
 * Two builders, because error-handler scope in the Java DSL is per-RouteBuilder — putting both
 * routes in one builder would give them the same clause and prove nothing.
 */
public class OwnershipProbe extends ProbeSupport {

    /** Witnesses whether the step after the call ran at all. */
    private final List<String> callerResumed = new CopyOnWriteArrayList<>();

    @Override
    protected RouteBuilder[] createRouteBuilders() {
        var caller = new RouteBuilder() {
            @Override
            public void configure() {
                onException(Boom.class).handled(true).setBody(constant("CAUGHT-BY-CALLER"));

                from("direct:caller")
                        .to("direct:callee-that-owns")
                        .process(ex -> callerResumed.add("yes"))
                        .setBody(constant("CALLER-CONTINUED"));

                from("direct:caller-of-declining")
                        .to("direct:callee-that-declines")
                        .setBody(constant("CALLER-CONTINUED"));
            }
        };

        var callee = new RouteBuilder() {
            @Override
            public void configure() {
                onException(Boom.class).handled(true).setBody(constant("CAUGHT-BY-CALLEE"));

                from("direct:callee-that-owns")
                        .process(ex -> {
                            throw new Boom();
                        });
            }
        };

        var decliningCallee = new RouteBuilder() {
            @Override
            public void configure() {
                // The only way to decline ownership.
                errorHandler(noErrorHandler());

                from("direct:callee-that-declines")
                        .process(ex -> {
                            throw new Boom();
                        });
            }
        };

        return new RouteBuilder[] { caller, callee, decliningCallee };
    }

    @Test
    void aCalleeThatOwnsTheErrorDecidesTheOutcome_andTheCallerDoesNotResume() {
        var body = template.requestBody("direct:caller", "in", String.class);

        assertThat(body)
                .describedAs("the callee's clause fired, and the caller's never got a look")
                .isEqualTo("CAUGHT-BY-CALLEE");
        assertThat(callerResumed)
                .describedAs("the step after .to(callee) must not have run — checking the body "
                        + "alone would only restate the assertion above")
                .isEmpty();
    }

    @Test
    void aCalleeThatDeclines_handsTheDecisionToTheCaller() {
        var body = template.requestBody("direct:caller-of-declining", "in", String.class);

        assertThat(body).isEqualTo("CAUGHT-BY-CALLER");
    }
}
