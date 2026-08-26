package sandbox;

import org.apache.camel.builder.RouteBuilder;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Whether {@link MarkRecovered} belongs first or last in a {@code doCatch} body.
 * <p>
 * Two things differ, and both are about the catch body itself rather than what comes after it: a
 * route the body calls can only map its own failures on an unclaimed exchange, and a failure of the
 * body can only be mapped upstream if it escapes unclaimed.
 */
public class ContinuePlacementProbe extends ProbeSupport {

    private static final MarkRecovered RECOVERED = MarkRecovered.INSTANCE;

    public static class CompensationFailed extends RuntimeException {
    }

    public static class LateBoom extends RuntimeException {
    }

    private final List<String> trace = new CopyOnWriteArrayList<>();

    @Override
    protected RouteBuilder[] createRouteBuilders() {
        // No clauses, so the default error handler claims and leaves the exception live.
        var stage = new RouteBuilder() {
            @Override
            public void configure() {
                from("direct:claiming-stage")
                        .process(ex -> {
                            throw new Boom();
                        });
            }
        };

        // Its own builder, so it owns its clause the way a real compensation route would.
        var compensator = new RouteBuilder() {
            @Override
            public void configure() {
                onException(CompensationFailed.class)
                        .handled(true)
                        .process(ex -> trace.add("compensator-clause-fired"))
                        .setBody(constant("COMP-BODY"));

                from("direct:compensate")
                        .process(ex -> {
                            throw new CompensationFailed();
                        });
            }
        };

        var caller = new RouteBuilder() {
            @Override
            public void configure() {
                onException(LateBoom.class)
                        .handled(true)
                        .process(ex -> trace.add("caller-mapped-late"))
                        .setBody(constant("MAPPED-LATE"));

                from("direct:reset-first")
                        .doTry()
                            .to("direct:claiming-stage")
                        .doCatch(Boom.class)
                            .process(RECOVERED)
                            .to("direct:compensate")
                        .end()
                        .process(ex -> trace.add("resumed"));

                // Does a handling compensation route stop the rest of the CATCH BODY too, not
                // just the step after end()?
                from("direct:reset-first-with-more-after")
                        .doTry()
                            .to("direct:claiming-stage")
                        .doCatch(Boom.class)
                            .process(RECOVERED)
                            .to("direct:compensate")
                            .process(ex -> trace.add("still-inside-catch"))
                        .end()
                        .process(ex -> trace.add("resumed"));

                from("direct:reset-last")
                        .doTry()
                            .to("direct:claiming-stage")
                        .doCatch(Boom.class)
                            .to("direct:compensate")
                            .process(RECOVERED)
                        .end()
                        .process(ex -> trace.add("resumed"));

                // The mapping clause has to be in a CALLING route: shouldWrapInErrorHandler
                // excludes TryDefinition itself, not only its children, so a failure escaping a
                // doTry block never reaches its own route's clauses.
                from("direct:outer-of-reset")
                        .to("direct:throws-after-reset");

                from("direct:outer-without-reset")
                        .to("direct:throws-without-reset");

                from("direct:throws-after-reset")
                        .doTry()
                            .to("direct:claiming-stage")
                        .doCatch(Boom.class)
                            .process(RECOVERED)
                            .process(ex -> {
                                throw new LateBoom();
                            })
                        .end();

                from("direct:throws-without-reset")
                        .doTry()
                            .to("direct:claiming-stage")
                        .doCatch(Boom.class)
                            .process(ex -> {
                                throw new LateBoom();
                            })
                        .end();
            }
        };

        return new RouteBuilder[] { stage, compensator, caller };
    }

    @Test
    void first_letsARouteTheBodyCallsMapItsOwnFailure() {
        var out = template.request("direct:reset-first", ex -> ex.getIn().setBody("in"));

        assertThat(trace)
                .describedAs("the compensation route's own clause fired, so its failure did not "
                        + "escape into the caller. 'resumed' does not follow: a callee that HANDLES "
                        + "its error stops the caller either way, which the reset does not change")
                .containsExactly("compensator-clause-fired");
        assertThat(out.getException()).isNull();
    }

    @Test
    void aHandlingCompensationRouteEndsTheCatchBodyAndTheRouteWithItsOwnBody() {
        var out = template.request("direct:reset-first-with-more-after",
                ex -> ex.getIn().setBody("in"));

        assertThat(trace)
                .describedAs("resolved is a term in the pipeline's between-steps gate, and the "
                        + "catch body is a pipeline too — so the step after the call does not run "
                        + "either, not just the step after end()")
                .containsExactly("compensator-clause-fired");
        assertThat(out.getMessage().getBody(String.class))
                .describedAs("and the response is whatever the compensation's clause set")
                .isEqualTo("COMP-BODY");
        assertThat(out.getException())
                .describedAs("on a clean exchange, so at an edge this body actually ships")
                .isNull();
    }

    @Test
    void last_leavesTheBodyRunningInsideTheJailItIsMeantToEscape() {
        var out = template.request("direct:reset-last", ex -> ex.getIn().setBody("in"));

        assertThat(trace)
                .describedAs("still claimed while the body runs, so the compensation route's clause "
                        + "never fires and its failure escapes unmapped — and the reset placed "
                        + "after it never runs at all")
                .isEmpty();
        assertThat(out.getException())
                .describedAs("the compensation's failure replaces everything")
                .isInstanceOf(CompensationFailed.class);
    }

    @Test
    void first_alsoLeavesAFailureOfTheBodyMappableUpstream() {
        var out = template.request("direct:outer-of-reset", ex -> ex.getIn().setBody("in"));

        assertThat(trace).containsExactly("caller-mapped-late");
        assertThat(out.getMessage().getBody(String.class)).isEqualTo("MAPPED-LATE");
    }

    @Test
    void withoutTheResetTheSameFailureCannotBeMapped() {
        var out = template.request("direct:outer-without-reset", ex -> ex.getIn().setBody("in"));

        assertThat(trace)
                .describedAs("the claim from the stage is still on the exchange, so the clause is "
                        + "closed to a failure that has nothing to do with it")
                .isEmpty();
        assertThat(out.getException()).isInstanceOf(LateBoom.class);
    }
}
