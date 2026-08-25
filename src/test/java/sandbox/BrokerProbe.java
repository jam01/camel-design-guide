package sandbox;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.RedeliveryPolicy;
import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.activemq.ActiveMQComponent;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What a broker consumer does with a failure, now that there is a broker to ask.
 * <p>
 * Everything the guide says about in-only exchanges — that handling a failure acknowledges and
 * drops the message, that leaving it alone produces redelivery and eventually a dead-letter queue,
 * that on a transacted consumer the rollback <em>is</em> the retry — was reasoned from the
 * mechanism until now. This is an in-JVM ActiveMQ, so it can be measured.
 */
public class BrokerProbe extends ProbeSupport {

    private final List<String> deliveries = new CopyOnWriteArrayList<>();
    private final List<String> deadLettered = new CopyOnWriteArrayList<>();

    @Override
    protected CamelContext createCamelContext() throws Exception {
        var context = super.createCamelContext();

        var cf = new ActiveMQConnectionFactory(
                "vm://" + getClass().getSimpleName() + System.nanoTime()
                + "?broker.persistent=false&broker.useJmx=false");

        // Three attempts, no backoff, so the dead-letter path is reachable inside a test.
        var policy = new RedeliveryPolicy();
        policy.setMaximumRedeliveries(2);
        policy.setInitialRedeliveryDelay(0);
        policy.setUseExponentialBackOff(false);
        cf.setRedeliveryPolicy(policy);

        var amq = new ActiveMQComponent();
        amq.setConnectionFactory(cf);
        context.addComponent("activemq", amq);
        return context;
    }

    @Override
    protected RouteBuilder createRouteBuilder() {
        return new RouteBuilder() {
            @Override
            public void configure() {
                // Left alone, on a plain consumer: the exception stays live.
                from("activemq:queue:plain-unhandled")
                        .process(ex -> {
                            deliveries.add("d");
                            throw new Boom();
                        });

                // The same, on a transacted consumer.
                from("activemq:queue:tx-unhandled?transacted=true")
                        .process(ex -> {
                            deliveries.add("d");
                            throw new Boom();
                        });

                // Mapped, exactly as an HTTP edge would map it.
                from("activemq:queue:tx-mapped?transacted=true")
                        .onException(Boom.class)
                            .handled(true)
                            .setBody(constant("mapped"))
                        .end()
                        .process(ex -> {
                            deliveries.add("d");
                            throw new Boom();
                        });

                // Mapped *and* marked, the recipe the guide prescribes for a transacted edge.
                from("activemq:queue:tx-marked?transacted=true")
                        .onException(Boom.class)
                            .handled(true)
                            .setBody(constant("mapped"))
                            .markRollbackOnly()
                        .end()
                        .process(ex -> {
                            deliveries.add("d");
                            throw new Boom();
                        });

                from("activemq:queue:ActiveMQ.DLQ")
                        .process(ex -> deadLettered.add(
                                String.valueOf(ex.getIn().getHeader("originalDestination"))));
            }
        };
    }

    private void settle(int expected) throws Exception {
        for (int i = 0; i < 60 && deliveries.size() < expected; i++) {
            Thread.sleep(25);
        }
        Thread.sleep(250);   // let any further deliveries arrive, so "exactly" means something
    }

    @Test
    void onAPlainConsumer_anUnhandledFailureIsAcknowledgedAndLost() throws Exception {
        template.sendBody("activemq:queue:plain-unhandled", "in");
        settle(1);

        assertThat(deliveries)
                .describedAs("delivered once and never again, despite the exception still being "
                        + "live. Without a transacted session the message is acknowledged whatever "
                        + "the listener does, so there is nothing to redeliver.")
                .hasSize(1);
        assertThat(deadLettered)
                .describedAs("and it did not reach the dead-letter queue either — it is simply gone")
                .isEmpty();
    }

    @Test
    void onATransactedConsumer_anUnhandledFailureIsRedeliveredThenDeadLettered() throws Exception {
        template.sendBody("activemq:queue:tx-unhandled", "in");
        settle(3);

        assertThat(deliveries)
                .describedAs("the same route and the same failure, one option different: the "
                        + "transaction rolls back, which un-acknowledges the message, so the broker "
                        + "brings it back until its policy is exhausted")
                .hasSize(3);
        assertThat(deadLettered).describedAs("and then the DLQ catches it").hasSize(1);
    }

    @Test
    void mappingTheFailure_acknowledgesAndDropsTheMessage() throws Exception {
        template.sendBody("activemq:queue:tx-mapped", "in");
        settle(1);

        assertThat(deliveries)
                .describedAs("handled(true) cleared the exception, so the transaction committed and "
                        + "the message was acknowledged — the construct that returns an error "
                        + "response over HTTP silently discards the message here")
                .hasSize(1);
        assertThat(deadLettered).isEmpty();
    }

    @Test
    void addingTheRollbackMark_bringsTheRedeliveryBack() throws Exception {
        template.sendBody("activemq:queue:tx-marked", "in");
        settle(3);

        assertThat(deliveries)
                .describedAs("the mark aborts the transaction even though no exception survives, so "
                        + "on a transacted consumer the rollback and the retry are one event")
                .hasSize(3);
        assertThat(deadLettered).hasSize(1);
    }
}
