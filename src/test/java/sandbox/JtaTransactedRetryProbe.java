package sandbox;

import com.arjuna.ats.jta.TransactionManager;
import jakarta.transaction.Status;
import jakarta.transaction.Synchronization;
import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.jta.JtaTransactionPolicy;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
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
 * policy because that is the one the application runs.
 */
public class JtaTransactedRetryProbe extends ProbeSupport {

    private final List<String> builderAttempts = new CopyOnWriteArrayList<>();
    private final List<String> clauseAttempts = new CopyOnWriteArrayList<>();
    private final List<String> routeAttempts = new CopyOnWriteArrayList<>();
    private final List<String> outcomes = new CopyOnWriteArrayList<>();

    public static class BuilderRetry extends RuntimeException {
    }

    public static class ClauseRetry extends RuntimeException {
    }

    public static class RouteRetry extends RuntimeException {
    }

    /** Quarkus's TransactionalJtaTransactionPolicy.runWithTransaction, as in {@link JtaFidelityProbe}. */
    private class Required extends JtaTransactionPolicy {
        @Override
        public void run(Runnable runnable) throws Throwable {
            var tm = TransactionManager.transactionManager();
            boolean isNew = tm.getStatus() == Status.STATUS_NO_TRANSACTION
                    || tm.getStatus() == Status.STATUS_MARKED_ROLLBACK;
            if (isNew) {
                tm.begin();
                tm.getTransaction().registerSynchronization(new Synchronization() {
                    @Override
                    public void beforeCompletion() {
                    }

                    @Override
                    public void afterCompletion(int status) {
                        outcomes.add(status == Status.STATUS_COMMITTED ? "COMMITTED" : "ROLLED_BACK");
                    }
                });
            }
            try {
                runnable.run();
            } catch (Throwable e) {
                if (isNew) {
                    tm.rollback();
                } else {
                    tm.setRollbackOnly();
                }
                throw e;
            }
            if (isNew) {
                tm.commit();
            }
        }
    }

    static {
        try {
            var dir = Files.createTempDirectory("narayana");
            dir.toFile().deleteOnExit();
            System.setProperty("ObjectStoreEnvironmentBean.objectStoreDir", dir.toString());
            System.setProperty("com.arjuna.ats.arjuna.objectstore.objectStoreDir", dir.toString());
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @Override
    protected CamelContext createCamelContext() throws Exception {
        var context = super.createCamelContext();
        context.getRegistry().bind("PROPAGATION_REQUIRED_JTA", new Required());
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
        // decision and Narayana's execution of it, not the data. The application's datasource is enlisted.
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
