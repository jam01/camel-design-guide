package sandbox;

import org.apache.camel.builder.RouteBuilder;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * CANDIDATE UPSTREAM ISSUE, and the reason it is not filed yet. Kept as the record of what was
 * tried, so this is not re-derived from scratch.
 * <p>
 * <b>The candidate.</b> An {@code onException} clause that fires and sets {@code handled(true)} has
 * its decision overridden and the exception restored, because {@code errorHandlerHandled} was
 * pinned {@code false} by an earlier, unrelated, already-resolved failure. That is narrower and
 * harder to defend than "claims persist", which was raised and closed as by design in CAMEL-19441.
 * <p>
 * <b>Why it is not filed.</b> No route shape has been found that reaches it without the
 * application first clearing {@code failureHandled} by hand. Without that, the claim suppresses the
 * clause outright and you are back in the closed-as-by-design case. Attempts that did not work:
 * a plain called-route failure then a later throw (clause never runs); {@code enrich}, whose
 * resource failure propagates rather than aggregating. Upstream would fairly answer that poking
 * internal state and getting surprising results is not a defect.
 * <p>
 * <b>What would make it filable.</b> A shape where a legitimate construct leaves
 * {@code errorHandlerHandled} pinned while the exchange is unclaimed. The copy constructor is the
 * most promising lead: {@code AbstractExchange(AbstractExchange parent)} propagates
 * {@code errorHandlerHandled}, {@code rollbackOnly}, {@code rollbackOnlyLast}, {@code routeStop}
 * and {@code redeliveryExhausted} — and does not propagate {@code failureHandled}. Two flags that
 * jointly decide whether a failure can be mapped are copied inconsistently. Find a route where a
 * copy is routed onward and then fails, and the case makes itself.
 */
public class CandidateIssueProbe extends ProbeSupport {

    public static class FirstFailure extends RuntimeException {
    }

    public static class SecondFailure extends RuntimeException {
    }

    private final List<String> clauseRan = new CopyOnWriteArrayList<>();

    @Override
    protected RouteBuilder[] createRouteBuilders() {
        var caller = new RouteBuilder() {
            @Override
            public void configure() {
                onException(SecondFailure.class)
                        .handled(true)
                        .process(ex -> clauseRan.add("yes"))
                        .setBody(constant("MAPPED"));

                // Control: first failure inline, so nothing claims.
                from("direct:inline-first")
                        .doTry()
                            .process(ex -> {
                                throw new FirstFailure();
                            })
                        .doCatch(FirstFailure.class)
                        .end()
                        .process(ex -> {
                            throw new SecondFailure();
                        });

                // The known, closed-as-by-design case.
                from("direct:route-first")
                        .doTry()
                            .to("direct:fails")
                        .doCatch(FirstFailure.class)
                        .end()
                        .process(ex -> {
                            throw new SecondFailure();
                        });

                // The candidate — reachable only after clearing the claim by hand.
                from("direct:route-first-then-unclaim")
                        .doTry()
                            .to("direct:fails")
                        .doCatch(FirstFailure.class)
                        .end()
                        .process(ex -> ex.getExchangeExtension().setFailureHandled(false))
                        .process(ex -> {
                            throw new SecondFailure();
                        });
            }
        };

        var callee = new RouteBuilder() {
            @Override
            public void configure() {
                from("direct:fails")
                        .process(ex -> {
                            throw new FirstFailure();
                        });
            }
        };

        return new RouteBuilder[] { caller, callee };
    }

    @Test
    void control_anInlineFirstFailureLeavesTheSecondFullyMappable() {
        var out = template.request("direct:inline-first", ex -> ex.getIn().setBody("in"));

        assertThat(clauseRan).containsExactly("yes");
        assertThat(out.getMessage().getBody(String.class)).isEqualTo("MAPPED");
        assertThat(out.getException())
                .describedAs("this is the behaviour the other two are measured against")
                .isNull();
    }

    @Test
    void known_aCalledRouteFirstFailureSuppressesTheClauseEntirely() {
        var out = template.request("direct:route-first", ex -> ex.getIn().setBody("in"));

        assertThat(clauseRan)
                .describedAs("the clause never runs — this is CAMEL-19441 territory, closed as by "
                        + "design, and not worth re-filing")
                .isEmpty();
        assertThat(out.getException()).isInstanceOf(SecondFailure.class);
    }

    @Test
    void candidate_afterUnclaimingTheClauseRunsAndIsThenOverridden() {
        var out = template.request("direct:route-first-then-unclaim", ex -> ex.getIn().setBody("in"));

        assertThat(clauseRan)
                .describedAs("with the claim cleared the clause IS reached and does run")
                .containsExactly("yes");
        assertThat(out.getMessage().getBody(String.class))
                .describedAs("and sets the mapped body it was asked for")
                .isEqualTo("MAPPED");
        assertThat(out.getException())
                .describedAs("yet handled(true) is overridden and the exception restored, from a "
                        + "flag pinned by a resolved unrelated failure. Indefensible on its own "
                        + "terms — but reachable only because the route touched internal state, "
                        + "which is why this is a candidate and not a filed ticket.")
                .isInstanceOf(SecondFailure.class);
    }
}
