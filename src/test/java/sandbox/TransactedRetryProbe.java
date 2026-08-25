package sandbox;

import org.apache.camel.builder.RouteBuilder;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What a clause's redelivery costs when the route is transacted.
 * <p>
 * Retry and transactions are configured independently and interact anyway: the open question was
 * whether N attempts mean N transactions or one. Under Spring the transaction wraps the redelivery
 * handler, so the answer here is not the answer under camel-jta, whose javadoc says each attempt
 * gets a fresh transaction.
 */
public class TransactedRetryProbe extends ProbeSupport {

    private final List<String> attempts = new CopyOnWriteArrayList<>();
    private final List<String> outcomes = new CopyOnWriteArrayList<>();

    public static class Retryable extends RuntimeException {
    }

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
                onException(Retryable.class)
                        .maximumRedeliveries(2).redeliveryDelay(0)
                        .handled(true)
                        .setBody(constant("gave-up"));

                from("direct:tx-retry")
                        .transacted()
                        .process(ex -> watch())
                        .to(insertRow("before"))
                        .process(ex -> {
                            attempts.add("a");
                            throw new Retryable();
                        });
            }
        };
    }

    @Test
    void everyAttemptSharesOneTransaction_andTheWriteBeforeThemIsNotRepeated() throws Exception {
        template.request("direct:tx-retry", ex -> ex.getIn().setBody("in"));

        assertThat(attempts).describedAs("three attempts at the failing step").hasSize(3);
        assertThat(outcomes)
                .describedAs("but ONE transaction, not three — under Spring the boundary is outside "
                        + "the redelivery loop, so every attempt runs inside the same transaction "
                        + "and there is a single commit decision at the end. camel-jta's javadoc "
                        + "says the opposite for its own handler: a fresh transaction per attempt.")
                .hasSize(1);
        assertThat(notes())
                .describedAs("the write before the failing step happened once and survived, because "
                        + "handled(true) with no mark cleared the exception before the boundary "
                        + "looked — retries do not change that calculus, they just delay it")
                .containsExactly("before");
    }
}
