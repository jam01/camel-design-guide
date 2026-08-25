package sandbox;

import org.apache.camel.builder.RouteBuilder;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What {@code continued(true)} does to a transaction it is sitting inside.
 * <p>
 * The guide warns never to put one above a transacted stage, on the grounds that it erases the
 * rollback mark. That came from reading {@code prepareExchangeForContinue}; this measures it.
 */
public class ContinuedProbe extends ProbeSupport {

    private final List<String> outcomes = new CopyOnWriteArrayList<>();
    private final List<String> steps = new CopyOnWriteArrayList<>();

    private void watch() {
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCompletion(int status) {
                outcomes.add(status == TransactionSynchronization.STATUS_COMMITTED
                        ? "COMMITTED" : "ROLLED_BACK");
            }
        });
    }

    @Override
    protected RouteBuilder createRouteBuilder() {
        return new RouteBuilder() {
            @Override
            public void configure() {
                onException(Boom.class).continued(true);

                from("direct:continue-plain")
                        .transacted()
                        .process(ex -> watch())
                        .to(insertRow("before"))
                        .process(ex -> {
                            throw new Boom();
                        })
                        .to(insertRow("after"));

                // Mark and throw in the SAME step, so the failure reaches the clause before the
                // mark has had a chance to stop the route.
                from("direct:mark-and-throw-together")
                        .transacted()
                        .process(ex -> watch())
                        .to(insertRow("before"))
                        .process(ex -> {
                            ex.setRollbackOnly(true);
                            throw new Boom();
                        })
                        .to(insertRow("after"));

                // Mark in one step, throw in the next — the route never gets that far. Witnessed
                // by which steps ran, not by rows: a rollback would erase the rows either way.
                from("direct:mark-then-throw")
                        .transacted()
                        .process(ex -> watch())
                        .process(ex -> steps.add("ran"))
                        .process(ex -> {
                            ex.setRollbackOnly(true);
                            steps.add("marked");
                        })
                        .process(ex -> {
                            steps.add("threw");
                            throw new Boom();
                        })
                        .process(ex -> steps.add("after"));
            }
        };
    }

    @Test
    void continuedSkipsTheFailingStepAndRunsTheRest() throws Exception {
        template.request("direct:continue-plain", ex -> ex.getIn().setBody("in"));

        assertThat(notes())
                .describedAs("the step that threw is skipped and the route carries on from the "
                        + "next one — the only construct that resumes inside the failing route")
                .containsExactly("before", "after");
        assertThat(outcomes).containsExactly("COMMITTED");
    }

    @Test
    void continuedErasesARollbackMarkSetByTheStepThatFailed() throws Exception {
        template.request("direct:mark-and-throw-together", ex -> ex.getIn().setBody("in"));

        assertThat(outcomes)
                .describedAs("the step asked for a rollback and then threw, and the transaction "
                        + "COMMITTED: prepareExchangeForContinue calls setRollbackOnly(false), so a "
                        + "clause that never mentions transactions silently revoked the abort")
                .containsExactly("COMMITTED");
        assertThat(notes())
                .describedAs("and the route carried on past it")
                .containsExactly("before", "after");
    }

    @Test
    void aMarkSetInAnEarlierStepStopsTheRouteBeforeAnythingCanContinue() throws Exception {
        template.request("direct:mark-then-throw", ex -> ex.getIn().setBody("in"));

        assertThat(steps)
                .describedAs("the mark halts routing at the very next step, so the throw never "
                        + "happens, no clause fires, and there is nothing for continued(true) to "
                        + "erase. That is why the dangerous combination needs the mark and the "
                        + "failure to come from the same step.")
                .containsExactly("ran", "marked");
        assertThat(outcomes).containsExactly("ROLLED_BACK");
    }
}
