package sandbox;

import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Whether a caller can turn a callee's failure into one of its own.
 * <p>
 * {@link TransactedCatchProbe} establishes that doTry/doCatch reaches a failure a transacted callee
 * claimed, and that the caller resumes after {@code end()}. The open question is what the caller can
 * then <em>do</em> with it. The natural move is to throw the domain exception the edge already knows
 * how to map — a 409, a 503 — rather than duplicating the mapping at the call site.
 * <p>
 * It does not work by default, and the reason is the claim rather than the catch: the callee sets
 * {@code failureHandled} on the exchange, {@code RedeliveryErrorHandler.isDone} treats a failure-handled
 * exchange as finished, and that verdict outlives the doCatch. Every later exception on the same
 * exchange is skipped by the clauses and leaves the route live — which at an HTTP edge is an empty
 * {@code text/plain} 500, because a failed exchange never renders its own body.
 * <p>
 * The claim is resettable, and that is what makes the throw work.
 */
public class CatchRethrowProbe extends ProbeSupport {

    private final List<String> mapped = new CopyOnWriteArrayList<>();
    private final List<String> seen = new CopyOnWriteArrayList<>();
    private final List<String> resumed = new CopyOnWriteArrayList<>();

    public static class CalleeFailed extends RuntimeException {
    }

    public static class Translated extends RuntimeException {
    }

    public static class Verdict extends RuntimeException {
    }


    @Override
    protected CamelContext createCamelContext() throws Exception {
        var context = super.createCamelContext();
        context.getRegistry().bind("PROPAGATION_REQUIRED_JTA", new NarayanaRequiredPolicy());
        return context;
    }

    @Override
    protected RouteBuilder createRouteBuilder() {
        return new RouteBuilder() {
            @Override
            public void configure() {
                // The edge's mapping: what a domain exception is supposed to become.
                onException(Translated.class)
                        .handled(true)
                        .process(ex -> mapped.add("translated"))
                        .setBody(constant("503"));

                onException(Verdict.class)
                        .handled(true)
                        .process(ex -> mapped.add("verdict"))
                        .setBody(constant("409"));

                from("direct:transacted-callee")
                        .transacted("PROPAGATION_REQUIRED_JTA")
                        .process(ex -> {
                            throw new CalleeFailed();
                        });

                from("direct:plain-callee")
                        .process(ex -> {
                            throw new CalleeFailed();
                        });

                // A caller that catches the callee's failure and raises its own in its place.
                from("direct:rethrow-after-transacted-callee")
                        .doTry()
                            .to("direct:transacted-callee")
                        .doCatch(Exception.class)
                            .process(ex -> {
                                throw new Translated();
                            })
                        .end()
                        .setBody(constant("not-reached"));

                // Same, but the callee is an ordinary route rather than a transaction boundary.
                from("direct:rethrow-after-plain-callee")
                        .doTry()
                            .to("direct:plain-callee")
                        .doCatch(Exception.class)
                            .process(ex -> {
                                throw new Translated();
                            })
                        .end()
                        .setBody(constant("not-reached"));

                // Raised after doTry/doCatch closed, on a call that SUCCEEDED — the control for
                // whether a completed catch poisons everything downstream of it.
                from("direct:throw-after-successful-call")
                        .doTry()
                            .to("direct:noop")
                        .doCatch(Exception.class)
                            .log("unreachable")
                        .end()
                        .process(ex -> {
                            throw new Verdict();
                        });

                from("direct:noop").log("ok");

                // The case the application actually relies on: the failure is raised inline, so nothing
                // claims it, and the catch block raises a domain exception in its place.
                // The untested cell: a caller, whose callee's doCatch caught a CLAIMED failure and then
                // raised its own. Distinct from direct:calls-the-rethrower, where the failure was inline.
                from("direct:calls-the-rethrower-after-claimed")
                        .to("direct:rethrow-after-transacted-callee")
                        .process(ex -> resumed.add("caller-resumed"));

                // How much of the exchange still works once a claim is on it: does a LATER doTry/doCatch
                // still catch, does routing still reach the end, and is a later failure still unmappable.
                from("direct:life-after-a-claim")
                        .doTry()
                            .to("direct:transacted-callee")
                        .doCatch(Exception.class)
                            .process(ex -> resumed.add("first-catch"))
                        .end()
                        .process(ex -> resumed.add("between " + stamps(ex)))
                        .doTry()
                            .process(ex -> {
                                throw new CalleeFailed();
                            })
                        .doCatch(Exception.class)
                            .process(ex -> resumed.add("second-catch"))
                        .end()
                        .setHeader("rendered-by-hand", constant(503))
                        .process(ex -> resumed.add("reached-end"));

                from("direct:calls-the-rethrower")
                        .to("direct:inline-throw-then-rethrow-in-catch")
                        .process(ex -> resumed.add("caller-resumed"));

                from("direct:inline-throw-then-rethrow-in-catch")
                        .doTry()
                            .process(ex -> {
                                throw new CalleeFailed();
                            })
                        .doCatch(Exception.class)
                            .process(ex -> {
                                throw new Translated();
                            })
                        .end()
                        .setBody(constant("not-reached"));

                // What EXCEPTION_CAUGHT actually holds inside the caller's catch, for a JTA-transacted
                // callee. A caller that branches on the exception type to pick a status depends on this.
                from("direct:record-caught-type")
                        .doTry()
                            .to("direct:transacted-callee")
                        .doCatch(Exception.class)
                            .process(ex -> {
                                var caught = ex.getProperty(org.apache.camel.Exchange.EXCEPTION_CAUGHT,
                                        Throwable.class);
                                seen.add(caught == null ? "null" : caught.getClass().getName());
                            })
                        .end();

                // No callee at all: the failure is raised inline, so nothing ever claims it.
                from("direct:inline-throw-then-catch")
                        .doTry()
                            .process(ex -> {
                                throw new CalleeFailed();
                            })
                        .doCatch(Exception.class)
                            .process(ex -> seen.add("in-catch " + stamps(ex)))
                        .end()
                        .process(ex -> {
                            seen.add("after-end " + stamps(ex));
                            throw new Verdict();
                        });

                // Records what the exchange looks like at each point when a callee DID claim.
                from("direct:stamps-after-claimed-callee")
                        .doTry()
                            .to("direct:transacted-callee")
                        .doCatch(Exception.class)
                            .process(ex -> seen.add("in-catch " + stamps(ex)))
                        .end()
                        .process(ex -> seen.add("after-end " + stamps(ex)));

                // The same as the first route, but the claim the callee left on the exchange is
                // cleared before the new exception is raised.
                from("direct:rethrow-after-clearing-the-claim")
                        .doTry()
                            .to("direct:transacted-callee")
                        .doCatch(Exception.class)
                            .process(ex -> {
                                ex.getExchangeExtension().setFailureHandled(false);
                                ex.getExchangeExtension().setRedeliveryExhausted(false);
                                throw new Translated();
                            })
                        .end()
                        .setBody(constant("not-reached"));

                // Outside the try block, where the error handler does wrap, with the callee's claim
                // cleared first.
                from("direct:rethrow-outside-after-clearing-the-claim")
                        .doTry()
                            .to("direct:transacted-callee")
                        .doCatch(Exception.class)
                            .process(ex -> ex.setProperty("failed", Boolean.TRUE))
                        .end()
                        .filter(ex -> Boolean.TRUE.equals(ex.getProperty("failed")))
                            .process(ex -> {
                                ex.getExchangeExtension().setFailureHandled(false);
                                throw new Translated();
                            })
                        .end();

                // Raised after a doTry/doCatch that DID catch, but outside the catch block.
                from("direct:throw-after-catch-block")
                        .doTry()
                            .to("direct:transacted-callee")
                        .doCatch(Exception.class)
                            .log("caught")
                        .end()
                        .process(ex -> {
                            throw new Verdict();
                        });
            }
        };
    }

    @Test
    void noClauseFiresForAnExceptionRaisedInsideDoCatch_afterACalleeClaimed() {
        var out = template.request("direct:rethrow-after-transacted-callee", ex -> ex.getIn().setBody("in"));

        assertThat(mapped)
                .describedAs("the clause does NOT fire. The callee's claim is still on the exchange, "
                        + "and isDone() counts a failure-handled exchange as finished, so the new "
                        + "exception is never offered to the clauses.")
                .isEmpty();
        assertThat(out.getException())
                .describedAs("and it leaves the route unhandled, which is why this surfaced at the "
                        + "HTTP edge as an empty text/plain 500 rather than as a mapped body")
                .isInstanceOf(Translated.class);
    }

    @Test
    void theClaimComesFromTheCallee_notFromTheTransaction() {
        template.request("direct:rethrow-after-plain-callee", ex -> ex.getIn().setBody("in"));

        assertThat(mapped)
                .describedAs("an ordinary route claims its failures too, so this is not about the "
                        + "callee being a transaction boundary — any called route does it")
                .isEmpty();
    }

    @Test
    void norAfterTheCatchBlockHasClosed() {
        var out = template.request("direct:throw-after-catch-block", ex -> ex.getIn().setBody("in"));

        assertThat(mapped)
                .describedAs("the claim outlives the doCatch: an exception raised further down the "
                        + "route, well outside the catch block, is skipped for the same reason")
                .isEmpty();
        assertThat(out.getException()).isInstanceOf(Verdict.class);
    }

    @Test
    void aCatchWithNothingToClaimLeavesTheClausesWorking() {
        template.request("direct:inline-throw-then-catch", ex -> ex.getIn().setBody("in"));

        assertThat(mapped)
                .describedAs("STAMPS: " + seen + " -- doCatch itself does not spend the clauses; "
                        + "an inline failure has no callee to claim it")
                .containsExactly("verdict");
    }

    @Test
    void whatTheStampsLookLikeAfterAClaimedCallee() {
        template.request("direct:stamps-after-claimed-callee", ex -> ex.getIn().setBody("in"));

        assertThat(seen).describedAs("recorded for the record, not asserted on").hasSize(2);
        System.out.println("STAMPS after claimed callee: " + seen);
    }

    @Test
    void theCatchSeesTheOriginalExceptionUnwrapped_underJta() {
        template.request("direct:record-caught-type", ex -> ex.getIn().setBody("in"));

        assertThat(seen)
                .describedAs("what a caller branching on exception type will actually match against")
                .containsExactly(CalleeFailed.class.getName());
    }

    @Test
    void anExceptionThrownInsideDoCatchEscapesItsOwnRoutesClauses() {
        var out = template.request("direct:inline-throw-then-rethrow-in-catch", ex -> ex.getIn().setBody("in"));

        assertThat(mapped)
                .describedAs("the route's own clause does not fire, even though nothing claimed the "
                        + "exchange: children of a catch block are not wrapped in an error handler, so "
                        + "there is nothing there to consult a clause")
                .isEmpty();
        assertThat(out.getException())
                .describedAs("it comes back live and, importantly, UNCLAIMED -- which is what leaves it "
                        + "available to whoever called this route")
                .isInstanceOf(Translated.class);
        assertThat(stamps(out)).contains("claimed=false");
    }

    @Test
    void butTheCallersClauseDoesMapIt() {
        var out = template.request("direct:calls-the-rethrower", ex -> ex.getIn().setBody("in"));

        assertThat(mapped)
                .describedAs("because it escaped unclaimed, the first error handler to see it is the "
                        + "caller's, and that one maps it normally. So a route whose doCatch throws is "
                        + "mapped by whoever called it -- and if the caller carries the same clauses, as "
                        + "a shared HTTP base class arranges, it is indistinguishable from the route "
                        + "having mapped itself.")
                .containsExactly("translated");
        assertThat(out.getMessage().getBody(String.class)).isEqualTo("503");
        assertThat(out.getException())
                .describedAs("and it leaves clean, so an HTTP edge renders the mapped body")
                .isNull();
        assertThat(resumed)
                .describedAs("the caller does not resume past the call, as with any handled failure")
                .isEmpty();
    }

    @Test
    void norDoesTheCallersClauseMapAThrowAfterAClaimedCallee() {
        var out = template.request("direct:calls-the-rethrower-after-claimed", ex -> ex.getIn().setBody("in"));

        assertThat(mapped)
                .describedAs("the claim survives the intervening doCatch and suppresses the CALLER's "
                        + "clause as well. So the escape hatch in butTheCallersClauseDoesMapIt is only "
                        + "available when the failure inside the doTry was inline: once a called route "
                        + "claimed it, no handler anywhere maps a throw from that catch.")
                .isEmpty();
        assertThat(out.getException())
                .describedAs("it reaches the caller live, and at an HTTP edge that is the empty 500")
                .isInstanceOf(Translated.class);
        assertThat(stamps(out))
                .describedAs("and this is why: doCatch clears the EXCEPTION, which is what lets routing "
                        + "continue past end(), but it does not clear the CLAIM. The two are independent "
                        + "-- continuation is gated on isFailed/isRollbackOnly/errorHandlerHandled, none "
                        + "of which is failureHandled, while eligibility to be mapped is gated on "
                        + "failureHandled via RedeliveryErrorHandler.isDone. A route can therefore carry "
                        + "on normally and still be unmappable for the rest of its life.")
                .contains("claimed=true");
    }

    @Test
    void aClaimedExchangeStillRoutes_catchesAndSetsHeaders_itOnlyLosesMapping() {
        var out = template.request("direct:life-after-a-claim", ex -> ex.getIn().setBody("in"));

        assertThat(resumed)
                .describedAs("everything structural still works on a claimed exchange: a later "
                        + "doTry/doCatch still catches, routing still reaches the end of the route")
                .containsExactly("first-catch", "between exception=false claimed=true handled=false rollbackMark=false",
                        "second-catch", "reached-end");
        assertThat(out.getMessage().getHeader("rendered-by-hand"))
                .describedAs("and a response set by hand still lands -- setHeader/setBody do not consult "
                        + "the claim, which is why rendering works where throwing does not")
                .isEqualTo(503);
        assertThat(mapped)
                .describedAs("the ONLY thing lost is eligibility to be mapped by a clause, for the rest "
                        + "of the exchange's life")
                .isEmpty();
        assertThat(out.getException())
                .describedAs("and the exchange ends clean here, because nothing was left thrown")
                .isNull();
    }

    @Test
    void aClauseFiresNormallyWhenTheCallSucceeded() {
        template.request("direct:throw-after-successful-call", ex -> ex.getIn().setBody("in"));

        assertThat(mapped)
                .describedAs("the control: with nothing claimed, a doTry/doCatch upstream costs "
                        + "nothing and the clause maps the exception as usual")
                .containsExactly("verdict");
    }

    @Test
    void clearingTheClaimInsideTheCatchChangesNothing() {
        var out = template.request("direct:rethrow-after-clearing-the-claim", ex -> ex.getIn().setBody("in"));

        assertThat(mapped)
                .describedAs("still nothing, and no flag can fix it: ProcessorDefinitionHelper."
                        + "shouldWrapInErrorHandler skips TryDefinition, CatchDefinition and every "
                        + "child of them, so a processor inside a doCatch has no error handler around "
                        + "it to consult a clause in the first place")
                .isEmpty();
        assertThat(out.getException()).isInstanceOf(Translated.class);
    }

    @Test
    void clearingTheClaimOutsideTheCatchRunsTheClauseButTheExchangeStillLeavesFailed() {
        var out = template.request("direct:rethrow-outside-after-clearing-the-claim", ex -> ex.getIn().setBody("in"));

        assertThat(mapped)
                .describedAs("outside the try block the error handler is back, and clearing the "
                        + "callee's claim with getExchangeExtension().setFailureHandled(false) does "
                        + "get the clause to run — both halves are required, outside the block and "
                        + "unclaimed")
                .containsExactly("translated");
        assertThat(out.getMessage().getBody(String.class))
                .describedAs("and it produces the mapped body")
                .isEqualTo("503");
        assertThat(out.getException())
                .describedAs("but the exchange still leaves the route FAILED, even though the clause "
                        + "ran to completion with handled(true) and the exchange was clean at the end "
                        + "of it (" + seen + "). At an HTTP edge that is fatal: the body is only read "
                        + "when no exception is present, so the caller gets an empty text/plain 500 "
                        + "and the 503 body is discarded. Translating a callee's failure into a mapped "
                        + "response is therefore not available to a caller at all — the route that "
                        + "owns the failure has to map it.")
                .isInstanceOf(Translated.class);
    }
}
