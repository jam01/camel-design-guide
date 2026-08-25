package sandbox;

import org.apache.camel.builder.RouteBuilder;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Whether splitting work into stages splits the commit.
 * <p>
 * The reason to break a stage out is usually that it should succeed or fail on its own. That takes
 * two decisions, not one: the propagation has to give the callee its own transaction, <em>and</em>
 * the callee has to abort in a way that does not travel back up. The rollback mark is set on the
 * exchange, so by default it does travel.
 */
public class NestedTransactionProbe extends ProbeSupport {

    @Override
    protected RouteBuilder[] createRouteBuilders() {
        var callers = new RouteBuilder() {
            @Override
            public void configure() {
                from("direct:transacted-caller")
                        .transacted()
                        .to(insertRow("caller"))
                        .to("direct:required-callee");

                // No boundary of its own, so the callee's is the only one.
                from("direct:plain-caller")
                        .to("direct:required-callee")
                        .to("direct:second-stage");

                from("direct:requires-new-caller")
                        .transacted()
                        .to(insertRow("caller"))
                        .to("direct:requires-new-callee");

                from("direct:requires-new-caller-last")
                        .transacted()
                        .to(insertRow("caller"))
                        .to("direct:requires-new-callee-last");

                from("direct:required-caller-last")
                        .transacted()
                        .to(insertRow("caller"))
                        .to("direct:required-callee-last");
            }
        };

        var callees = new RouteBuilder() {
            @Override
            public void configure() {
                // Owns the failure and aborts the whole exchange, as a stage at an edge would.
                onException(Boom.class)
                        .handled(true)
                        .setBody(constant("mapped"))
                        .markRollbackOnly();

                // Owns the failure and aborts only its own transaction.
                onException(OtherBoom.class)
                        .handled(true)
                        .setBody(constant("mapped"))
                        .markRollbackOnlyLast();

                from("direct:required-callee")
                        .transacted()
                        .to(insertRow("callee"))
                        .process(ex -> {
                            throw new Boom();
                        });

                from("direct:requires-new-callee")
                        .transacted("PROPAGATION_REQUIRES_NEW")
                        .to(insertRow("callee"))
                        .process(ex -> {
                            throw new Boom();
                        });

                from("direct:requires-new-callee-last")
                        .transacted("PROPAGATION_REQUIRES_NEW")
                        .to(insertRow("callee"))
                        .process(ex -> {
                            throw new OtherBoom();
                        });

                from("direct:required-callee-last")
                        .transacted()
                        .to(insertRow("callee"))
                        .process(ex -> {
                            throw new OtherBoom();
                        });

                from("direct:second-stage")
                        .transacted()
                        .to(insertRow("second"));
            }
        };

        return new RouteBuilder[] { callers, callees };
    }

    private void run(String uri) {
        template.request(uri, ex -> ex.getIn().setBody("in"));
    }

    @Test
    void aTransactedCallerAbsorbsATransactedCallee_soOneRollbackTakesEverything() throws Exception {
        run("direct:transacted-caller");

        assertThat(rows())
                .describedAs("with the default propagation there is one transaction, not two — "
                        + "breaking the stage into its own route bought no isolation at all")
                .isZero();
    }

    @Test
    void anUntransactedCaller_leavesEachStageWithItsOwnCommit() throws Exception {
        run("direct:plain-caller");

        assertThat(rows())
                .describedAs("the failing stage rolled back its own row, and the caller was stopped "
                        + "so the second stage never ran — separate transactions, separate outcomes")
                .isZero();
    }

    @Test
    void requiresNew_isNotEnough_becauseTheMarkTravelsOnTheExchange() throws Exception {
        run("direct:requires-new-caller");

        assertThat(rows())
                .describedAs("the callee got its own transaction, but markRollbackOnly() sets the "
                        + "mark on the exchange, which is shared — the caller's boundary read the "
                        + "same mark and rolled its own writes back too")
                .isZero();
    }

    /**
     * DOES NOT TRANSFER to camel-jta, but not for the reason once predicted here. The guess was
     * that JTA's two-term rollback condition would simply ignore the mark and commit. Measured
     * since, in {@link JtaFidelityProbe}, the mark is never read at all: under camel-jta the
     * transaction completes before the clause that sets it has even run. Do not quote this test
     * for a JTA stack — quote {@code JtaFidelityProbe}.
     */
    @Test
    void requiresNewPlusRollbackOnlyLast_isolatesTheCalleeProperly() throws Exception {
        run("direct:requires-new-caller-last");

        assertThat(notes())
                .describedAs("markRollbackOnlyLast() aborts the innermost transaction and clears "
                        + "the mark on the way out, so the CALLER's row is the one that survives. "
                        + "A count of 1 would be equally true of the opposite outcome.")
                .containsExactly("caller");
    }

    @Test
    void rollbackOnlyLast_isolatesNothing_whenThereIsOnlyOneTransactionToBeginWith() throws Exception {
        run("direct:required-caller-last");

        assertThat(rows())
                .describedAs("with the default propagation the callee never opened a transaction of "
                        + "its own, so the innermost one is the caller's — 'last' names the same "
                        + "transaction the caller is in, and both rows go")
                .isZero();
    }
}
