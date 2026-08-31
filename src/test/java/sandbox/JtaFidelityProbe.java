package sandbox;

import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The two findings that were known not to transfer from Spring's transaction manager to the JTA
 * one, settled against a real JTA transaction manager.
 * <p>
 * What is under test is <em>Camel's</em> rollback condition, in
 * {@code org.apache.camel.jta.TransactionErrorHandler}: Spring's rolls back on
 * {@code exception != null || isRollbackOnly() || isRollbackOnlyLast()} and the JTA one drops the
 * third term. The policy below is the faithful contract a real one implements — the runnable
 * throwing means roll back, returning means commit — so the decision being measured is Camel's,
 * and the transaction, the commit and the rollback are Narayana's.
 */
public class JtaFidelityProbe extends ProbeSupport {

    private final List<String> outcomes = new CopyOnWriteArrayList<>();

    /** Which error handler classes are on the stack when the transaction is opened. */
    private final Set<String> handlers = new ConcurrentSkipListSet<>();

    public static class Unmarked extends RuntimeException {
    }

    public static class RouteScoped extends RuntimeException {
    }

    public static class InCallee extends RuntimeException {
    }

    public static class InCalleeMarked extends RuntimeException {
    }

    /**
     * A faithful copy of Quarkus's {@code TransactionalJtaTransactionPolicy.runWithTransaction} —
     * begin if this is the outermost, run, roll back and rethrow on any throwable, otherwise
     * commit. A Quarkus application uses that class, so reproducing it here rather than inventing
     * a policy is what makes this probe say anything about a real JTA deployment.
     */


    @Override
    protected CamelContext createCamelContext() throws Exception {
        var context = super.createCamelContext();
        // Deliberately NOT named PROPAGATION_REQUIRED: ProbeSupport binds a Spring policy under
        // that name, and a bare .transacted() resolves it. Every route here must name this one
        // explicitly, or it silently runs under Spring — in the one probe whose point is not to.
        context.getRegistry().bind("PROPAGATION_REQUIRED_JTA", new NarayanaRequiredPolicy(outcomes::add, handlers::add));
        return context;
    }

    @Override
    protected RouteBuilder createRouteBuilder() {
        return new RouteBuilder() {
            @Override
            public void configure() {
                // Mapped to a response and aborted with the plain mark.
                onException(Boom.class)
                        .handled(true)
                        .process(ex -> outcomes.add("clause-ran"))
                        .setBody(constant("mapped"))
                        .markRollbackOnly();

                // Clauses for a failure thrown one direct: hop away, outside the boundary.
                onException(InCallee.class)
                        .handled(true)
                        .process(ex -> outcomes.add("clause-ran"))
                        .setBody(constant("mapped"));

                onException(InCalleeMarked.class)
                        .handled(true)
                        .process(ex -> outcomes.add("clause-ran"))
                        .setBody(constant("mapped"))
                        .markRollbackOnly();

                onException(Unmarked.class)
                        .handled(true)
                        .process(ex -> outcomes.add("clause-ran"))
                        .setBody(constant("mapped"));

                // Mapped and aborted with the "innermost only" mark — the disputed one.
                onException(OtherBoom.class)
                        .handled(true)
                        .process(ex -> outcomes.add("clause-ran"))
                        .setBody(constant("mapped"))
                        .markRollbackOnlyLast();

                from("direct:marked")
                        .transacted("PROPAGATION_REQUIRED_JTA")
                        .process(ex -> {
                            throw new Boom();
                        });

                from("direct:marked-last")
                        .transacted("PROPAGATION_REQUIRED_JTA")
                        .process(ex -> {
                            throw new OtherBoom();
                        });

                // The same failure, but the clause is attached to the route rather than the
                // builder — declared before .transacted(), which is the only valid placement.
                from("direct:route-scoped")
                        .onException(RouteScoped.class)
                            .handled(true)
                            .process(ex -> outcomes.add("clause-ran"))
                            .setBody(constant("mapped"))
                        .end()
                        .transacted("PROPAGATION_REQUIRED_JTA")
                        .process(ex -> {
                            throw new RouteScoped();
                        });

                from("direct:calls-out")
                        .transacted("PROPAGATION_REQUIRED_JTA")
                        .to("direct:untransacted-callee");

                from("direct:untransacted-callee")
                        .process(ex -> {
                            throw new InCallee();
                        });

                from("direct:calls-out-marked")
                        .transacted("PROPAGATION_REQUIRED_JTA")
                        .to("direct:untransacted-callee-marked");

                from("direct:untransacted-callee-marked")
                        .process(ex -> {
                            throw new InCalleeMarked();
                        });

                // Mapped with no mark at all — the arrangement the guide calls the original bug.
                from("direct:mapped-only")
                        .transacted("PROPAGATION_REQUIRED_JTA")
                        .process(ex -> {
                            throw new Unmarked();
                        });
            }
        };
    }

    @Test
    void underJta_theTransactionCompletesBeforeTheClauseEverRuns() {
        template.request("direct:marked", ex -> ex.getIn().setBody("in"));

        assertThat(outcomes)
                .describedAs("camel-jta nests the transaction INSIDE the redelivery handler, so the "
                        + "body's exception reaches the transaction first: it rolls back and only "
                        + "then is the clause dispatched. The clause is running after the outcome "
                        + "it appears to control has already been decided.")
                .containsExactly("ROLLED_BACK", "clause-ran");
    }

    @Test
    void underJta_markRollbackOnlyLastMakesNoDifference() {
        template.request("direct:marked-last", ex -> ex.getIn().setBody("in"));

        assertThat(outcomes)
                .describedAs("the disputed mark changes nothing here, but not for the reason "
                        + "predicted from the rollback condition — the transaction was over before "
                        + "the mark was set")
                .containsExactly("ROLLED_BACK", "clause-ran");
    }

    @Test
    void underJta_handledAloneDoesNotCommit_whichIsTheOppositeOfSpring() {
        template.request("direct:mapped-only", ex -> ex.getIn().setBody("in"));

        assertThat(outcomes)
                .describedAs("handled(true) with no rollback mark — the exact arrangement that "
                        + "commits under Spring and is the bug this whole guide started from. "
                        + "Under camel-jta it ROLLS BACK, because the exception reached the "
                        + "transaction before any clause could clear it.")
                .containsExactly("ROLLED_BACK", "clause-ran");
    }

    @Test
    void aRouteScopedClauseIsDispatchedInTheSamePlace_soItIsStillTooLate() {
        template.request("direct:route-scoped", ex -> ex.getIn().setBody("in"));

        assertThat(outcomes)
                .describedAs("scoping the clause to the route does not move it inside the "
                        + "transaction: the dispatcher is still the outer redelivery handler, so "
                        + "the ordering is unchanged and the clause is no more able to influence "
                        + "the outcome than a builder-scoped one")
                .containsExactly("ROLLED_BACK", "clause-ran");
    }

    @Test
    void theTransactionIsDrivenFromInsideTheRedeliveryHandler() {
        template.request("direct:mapped-only", ex -> ex.getIn().setBody("in"));

        assertThat(handlers)
                .describedAs("captured from the stack at the moment the transaction opens: the "
                        + "JTA transaction handler is being called BY a redelivery task, which is "
                        + "the nesting the class hierarchy predicts and the reason the clause "
                        + "cannot reach the transaction. Any stack that disagrees is running a "
                        + "different error handler, and that is the thing to check first.")
                .contains("org.apache.camel.jta.TransactionErrorHandler")
                .anyMatch(h -> h.contains("RedeliveryErrorHandler"));
    }

    @Test
    void aFailureInAnUntransactedCallee_isClearedBeforeTheBoundaryEverSeesIt() {
        template.request("direct:calls-out", ex -> ex.getIn().setBody("in"));

        assertThat(outcomes)
                .describedAs("the throw is one direct: hop outside the transacted route, so the "
                        + "callee's ORDINARY error handler dispatches the clause — not the "
                        + "transaction handler. The clause clears the exception, control returns "
                        + "clean, and the boundary evaluates a successful exchange and COMMITS. "
                        + "Same runtime and same clause as the rollback case; only the route the "
                        + "throw happened in was different.")
                .containsExactly("clause-ran", "COMMITTED");
    }

    @Test
    void andTheMarkIsWhatCarriesTheFailureBackAcrossTheBoundary() {
        template.request("direct:calls-out-marked", ex -> ex.getIn().setBody("in"));

        assertThat(outcomes)
                .describedAs("markRollbackOnly() sets the mark on the exchange, the exchange "
                        + "crosses back into the transacted route, and the boundary reads it. That "
                        + "is why the mark fixes this shape and why nothing else does.")
                .containsExactly("clause-ran", "ROLLED_BACK");
    }
}
