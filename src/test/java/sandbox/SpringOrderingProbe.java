package sandbox;

import org.apache.camel.builder.RouteBuilder;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Whether "the transaction finishes before the clause runs" is a fact about Camel or a fact about
 * which transaction error handler you got.
 * <p>
 * {@link JtaFidelityProbe} measured the JTA answer. This is the same measurement on Spring's
 * manager, where the class hierarchy is inverted: Spring's {@code TransactionErrorHandler} <em>is</em>
 * the {@code RedeliveryErrorHandler}, so clause dispatch happens inside the transaction template
 * rather than outside it.
 */
public class SpringOrderingProbe extends ProbeSupport {

    private final List<String> outcomes = new CopyOnWriteArrayList<>();

    /** Records commit/rollback from inside the active Spring transaction. */
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
                onException(Boom.class)
                        .handled(true)
                        .process(ex -> outcomes.add("clause-ran"))
                        .setBody(constant("mapped"));

                onException(OtherBoom.class)
                        .handled(true)
                        .process(ex -> outcomes.add("clause-ran"))
                        .setBody(constant("mapped"))
                        .markRollbackOnly();

                from("direct:mapped-only")
                        .transacted()
                        .process(ex -> watch())
                        .process(ex -> {
                            throw new Boom();
                        });

                from("direct:mapped-and-marked")
                        .transacted()
                        .process(ex -> watch())
                        .process(ex -> {
                            throw new OtherBoom();
                        });
            }
        };
    }

    @Test
    void underSpringTheClauseRunsFirst_whichIsWhyHandledAloneCommits() {
        template.request("direct:mapped-only", ex -> ex.getIn().setBody("in"));

        assertThat(outcomes)
                .describedAs("the opposite order to camel-jta. Spring's transaction handler IS the "
                        + "redelivery handler, so the clause is dispatched inside the transaction "
                        + "template — it runs first, clears the exception, and the boundary then "
                        + "commits a clean exchange. The ordering is a property of the manager, "
                        + "not of Camel.")
                .containsExactly("clause-ran", "COMMITTED");
    }

    @Test
    void andTheMarkIsReadBecauseTheClauseSetItInTime() {
        template.request("direct:mapped-and-marked", ex -> ex.getIn().setBody("in"));

        assertThat(outcomes)
                .describedAs("same order, and now the mark exists by the time the boundary looks")
                .containsExactly("clause-ran", "ROLLED_BACK");
    }
}
