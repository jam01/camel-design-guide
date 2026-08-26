package sandbox;

import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.processor.errorhandler.RedeliveryPolicy;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * How finely retry can be scoped: per builder, per route, per exception type, and per predicate.
 * <p>
 * Retry is the one part of error handling that is genuinely configurable at several levels at once,
 * and getting it wrong multiplies attempts rather than replacing them. Each probe counts actual
 * invocations of the failing step.
 */
public class RedeliveryScopeProbe extends ProbeSupport {

    private final List<String> attempts = new CopyOnWriteArrayList<>();

    public static class Transient extends RuntimeException { }

    public static class Poison extends RuntimeException { }

    public static class Conditional extends RuntimeException { }

    /** Deliberately has no clause anywhere, so only the error handler decides. */
    public static class Unmatched extends RuntimeException { }

    /** Matched by a clause that has no outputs of its own. */
    public static class Bodyless extends RuntimeException { }

    /** Matched by a clause with a body that points at a shared policy by name. */
    public static class Referenced extends RuntimeException { }

    @Override
    protected CamelContext createCamelContext() throws Exception {
        var context = super.createCamelContext();
        var shared = new RedeliveryPolicy();
        shared.setMaximumRedeliveries(2);
        shared.setRedeliveryDelay(0);
        context.getRegistry().bind("sharedPolicy", shared);
        return context;
    }

    private int count() {
        return attempts.size();
    }

    @Override
    protected RouteBuilder createRouteBuilder() {
        return new RouteBuilder() {
            @Override
            public void configure() {
                // The builder-wide floor: no retries at all.
                errorHandler(defaultErrorHandler().maximumRedeliveries(0));

                // By exception type.
                onException(Transient.class)
                        .maximumRedeliveries(2).redeliveryDelay(0)
                        .handled(true).setBody(constant("transient"));

                onException(Poison.class)
                        .handled(true).setBody(constant("poison"));

                // A clause with a body AND a policy, named rather than restated.
                onException(Referenced.class)
                        .redeliveryPolicyRef("sharedPolicy")
                        .handled(true).setBody(constant("referenced"));

                // Matches and handles, but declares no steps — so it has no outputs. The only
                // difference from the Poison clause above is that one adds a setBody.
                onException(Bodyless.class).handled(true);

                // Same type, two clauses: the predicate selects which applies.
                onException(Conditional.class).onWhen(header("retry").isEqualTo("yes"))
                        .maximumRedeliveries(2).redeliveryDelay(0)
                        .handled(true).setBody(constant("conditional-retried"));

                onException(Conditional.class)
                        .handled(true).setBody(constant("conditional-not-retried"));

                from("direct:transient").process(ex -> {
                    attempts.add("a");
                    throw new Transient();
                });

                from("direct:poison").process(ex -> {
                    attempts.add("a");
                    throw new Poison();
                });

                from("direct:conditional").process(ex -> {
                    attempts.add("a");
                    throw new Conditional();
                });

                // Its own handler, overriding the builder's for this route only.
                from("direct:route-scoped-unmatched")
                        .errorHandler(defaultErrorHandler().maximumRedeliveries(3).redeliveryDelay(0))
                        .process(ex -> {
                            attempts.add("a");
                            throw new Unmatched();
                        });

                from("direct:referenced")
                        .errorHandler(defaultErrorHandler().maximumRedeliveries(0))
                        .process(ex -> {
                            attempts.add("a");
                            throw new Referenced();
                        });

                from("direct:route-scoped-bodyless")
                        .errorHandler(defaultErrorHandler().maximumRedeliveries(3).redeliveryDelay(0))
                        .process(ex -> {
                            attempts.add("a");
                            throw new Bodyless();
                        });

                // Same route-scoped handler, but a builder clause matches this type.
                from("direct:route-scoped-matched")
                        .errorHandler(defaultErrorHandler().maximumRedeliveries(3).redeliveryDelay(0))
                        .process(ex -> {
                            attempts.add("a");
                            throw new Poison();
                        });
            }
        };
    }

    @Test
    void redeliveryCanBeScopedToAnExceptionType() {
        var body = template.requestBody("direct:transient", "in", String.class);

        assertThat(body).isEqualTo("transient");
        assertThat(count())
                .describedAs("the clause's own policy overrides the builder's floor of zero")
                .isEqualTo(3);
    }

    @Test
    void aDifferentTypeInTheSameBuilder_getsTheBuilderFloor() {
        template.requestBody("direct:poison", "in", String.class);

        assertThat(count())
                .describedAs("no policy on this clause, so the builder's zero applies — retry is "
                        + "per exception type, not per builder")
                .isEqualTo(1);
    }

    @Test
    void aPredicateCanSelectBetweenTwoClausesForTheSameType() {
        var retried = template.requestBodyAndHeader(
                "direct:conditional", "in", "retry", "yes", String.class);
        assertThat(retried).isEqualTo("conditional-retried");
        assertThat(count()).describedAs("onWhen matched, so the retrying clause applied").isEqualTo(3);

        attempts.clear();

        var notRetried = template.requestBodyAndHeader(
                "direct:conditional", "in", "retry", "no", String.class);
        assertThat(notRetried).isEqualTo("conditional-not-retried");
        assertThat(count())
                .describedAs("same exception type, same route, different retry policy — chosen by "
                        + "a predicate on the message")
                .isEqualTo(1);
    }

    @Test
    void aRouteCanCarryItsOwnHandler_withItsOwnRetryPolicy() {
        template.request("direct:route-scoped-unmatched", ex -> ex.getIn().setBody("in"));

        assertThat(count())
                .describedAs("the route's handler replaced the builder's for this route only — "
                        + "retry is scopeable per route, not just per builder")
                .isEqualTo(4);
    }

    @Test
    void aMatchingClauseWithNoPolicyOfItsOwn_silentlyDropsTheHandlersRetries() {
        template.request("direct:route-scoped-matched", ex -> ex.getIn().setBody("in"));

        assertThat(count())
                .describedAs("same route, same handler configured for 3 redeliveries — but a "
                        + "clause matched, and a clause that specifies no policy does not inherit "
                        + "the handler's. Writing onException(X).handled(true) turns retry off "
                        + "for X without ever mentioning retry.")
                .isEqualTo(1);
    }

    @Test
    void aMatchingClauseWithNoOutputs_doesInheritTheHandlersRetries() {
        template.request("direct:route-scoped-bodyless", ex -> ex.getIn().setBody("in"));

        assertThat(count())
                .describedAs("the reset applies only to a clause that has steps in it. A bare "
                        + "onException(X) inherits the handler's policy — so adding a single "
                        + "logging step to it is what silently turns retry off.")
                .isEqualTo(4);
    }

    @Test
    void aClauseWithABodyKeepsItsRetriesIfItNamesAPolicy() {
        var body = template.requestBody("direct:referenced", "in", String.class);

        assertThat(body).isEqualTo("referenced");
        assertThat(count())
                .describedAs("the reset only fires for a clause that declares no policy at all. "
                        + "Pointing at one by name takes a different branch entirely, so a clause "
                        + "can have both steps and retries — and the number lives in one place "
                        + "rather than being restated on every clause that grew a body.")
                .isEqualTo(3);
    }
}
