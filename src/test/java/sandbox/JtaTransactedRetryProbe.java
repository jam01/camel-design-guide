package sandbox;

import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Where redelivery has to be configured for a transacted route to actually retry, under camel-jta.
 * <p>
 * The question matters because the alternative to an in-route retry is telling the caller to repeat
 * the whole request — and when the request carried a document upload, that is a second upload of the
 * same bytes to buy one more attempt at two INSERTs.
 * <p>
 * {@link TransactedRetryProbe} answers the neighbouring question under Spring (N attempts, one
 * transaction). This one is about <em>whether the attempts happen at all</em>, and it uses the JTA
 * policy because that is the one a Quarkus application runs.
 */
public class JtaTransactedRetryProbe extends ProbeSupport {

    private final List<String> builderAttempts = new CopyOnWriteArrayList<>();
    private final List<String> clauseAttempts = new CopyOnWriteArrayList<>();
    private final List<String> routeAttempts = new CopyOnWriteArrayList<>();
    private final List<String> routeClauseAttempts = new CopyOnWriteArrayList<>();
    private final List<String> outcomes = new CopyOnWriteArrayList<>();

    public static class BuilderRetry extends RuntimeException {
    }

    public static class ClauseRetry extends RuntimeException {
    }

    public static class RouteRetry extends RuntimeException {
    }

    public static class RouteClauseRetry extends RuntimeException {
    }

    /** Quarkus's TransactionalJtaTransactionPolicy.runWithTransaction, as in {@link JtaFidelityProbe}. */


    @Override
    protected CamelContext createCamelContext() throws Exception {
        var context = super.createCamelContext();
        context.getRegistry().bind("PROPAGATION_REQUIRED_JTA", new NarayanaRequiredPolicy(outcomes::add));
        return context;
    }

    @Override
    protected RouteBuilder createRouteBuilder() {
        return new RouteBuilder() {
            @Override
            public void configure() {
                // Level 1: the builder's error handler. This is the one .transacted() replaces.
                errorHandler(defaultErrorHandler().maximumRedeliveries(2).redeliveryDelay(0));

                // Level 3: an exception clause, no outputs so nothing zeroes it.
                onException(ClauseRetry.class)
                        .maximumRedeliveries(2).redeliveryDelay(0);

                from("direct:jta-builder-retry")
                        .transacted("PROPAGATION_REQUIRED_JTA")
                        .to(insertRow("builder"))
                        .process(ex -> {
                            builderAttempts.add("a");
                            throw new BuilderRetry();
                        });

                from("direct:jta-clause-retry")
                        .transacted("PROPAGATION_REQUIRED_JTA")
                        .to(insertRow("clause"))
                        .process(ex -> {
                            clauseAttempts.add("a");
                            throw new ClauseRetry();
                        });

                // Level 3 again, but scoped to the route rather than the builder. Declared before
                // .transacted(), which is the only valid placement for a route-scoped clause.
                from("direct:jta-route-clause-retry")
                        .onException(RouteClauseRetry.class)
                            .maximumRedeliveries(2).redeliveryDelay(0)
                        .end()
                        .transacted("PROPAGATION_REQUIRED_JTA")
                        .to(insertRow("route-clause"))
                        .process(ex -> {
                            routeClauseAttempts.add("a");
                            throw new RouteClauseRetry();
                        });

                // Level 2: a route-scoped handler, declared before .transacted().
                from("direct:jta-route-retry")
                        .errorHandler(defaultErrorHandler().maximumRedeliveries(2).redeliveryDelay(0))
                        .transacted("PROPAGATION_REQUIRED_JTA")
                        .to(insertRow("route"))
                        .process(ex -> {
                            routeAttempts.add("a");
                            throw new RouteRetry();
                        });
            }
        };
    }

    @Test
    void aBuilderErrorHandlerBuysNoRetriesOnATransactedRoute() {
        template.request("direct:jta-builder-retry", ex -> ex.getIn().setBody("in"));

        assertThat(builderAttempts)
                .describedAs("ONE attempt. .transacted() installs camel-jta's TransactionErrorHandler "
                        + "in place of the builder's, and the maximumRedeliveries configured there "
                        + "goes with it. The declaration compiles, deploys, and silently does nothing "
                        + "— which is the trap: the route reads as though it retries twice.")
                .hasSize(1);
    }

    @Test
    void aClauseCarriesItsOwnRedeliveryOntoATransactedRoute() throws Exception {
        template.request("direct:jta-clause-retry", ex -> ex.getIn().setBody("in"));

        assertThat(clauseAttempts)
                .describedAs("THREE attempts. An exception clause's redelivery policy is consulted by "
                        + "whichever error handler is installed, and the transaction error handler is "
                        + "still a RedeliveryErrorHandler — so putting maximumRedeliveries on the "
                        + "clause is how a transacted route retries. This is the level that works.")
                .hasSize(3);
        assertThat(outcomes)
                .describedAs("and each attempt is its own transaction under camel-jta, so a retry "
                        + "starts from a clean slate rather than re-running inside the transaction "
                        + "the previous attempt already poisoned")
                .containsExactly("ROLLED_BACK", "ROLLED_BACK", "ROLLED_BACK");
        // Deliberately no assertion on rows(): as in JtaFidelityProbe, the H2 DataSource here is not
        // enlisted in the JTA transaction, so what this harness measures is Camel's commit/rollback
        // decision and Narayana's execution of it, not the data. A real datasource would be enlisted.
    }

    @Test
    void aRouteScopedClauseCarriesItsRedeliveryToo() {
        template.request("direct:jta-route-clause-retry", ex -> ex.getIn().setBody("in"));

        assertThat(routeClauseAttempts)
                .describedAs("scoping the clause to the route rather than the builder changes nothing: "
                        + "what survives .transacted() is being a clause, not where the clause is "
                        + "declared. Use whichever scope the route deserves.")
                .hasSize(3);
    }

    @Test
    void aRouteScopedErrorHandlerAlsoLosesToTheTransactionHandler() {
        template.request("direct:jta-route-retry", ex -> ex.getIn().setBody("in"));

        assertThat(routeAttempts)
                .describedAs("ONE attempt, for the same reason as the builder-scoped one: .transacted() "
                        + "wins over a route handler too. Only the clause level survives it.")
                .hasSize(1);
    }
}
