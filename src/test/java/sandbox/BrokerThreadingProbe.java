package sandbox;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.activemq.ActiveMQComponent;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Where a broker consumer's concurrency comes from, and what a slow consumer does to its producer.
 * <p>
 * The interesting contrast is with {@code seda:}. Both are queues at a thread boundary, but one
 * keeps its backlog inside the process — bounded, and hostile about it — while the other keeps it
 * in the broker and only pushes back when the broker itself runs out of room.
 */
public class BrokerThreadingProbe extends ProbeSupport {

    private BrokerService broker;

    private final Set<String> consumerThreads = ConcurrentHashMap.newKeySet();
    private final AtomicInteger inFlight = new AtomicInteger();
    private final AtomicInteger peakInFlight = new AtomicInteger();
    private final CountDownLatch threeArrived = new CountDownLatch(3);
    private final CountDownLatch release = new CountDownLatch(1);
    private final AtomicInteger slowConsumed = new AtomicInteger();
    private final AtomicInteger timedOut = new AtomicInteger();

    @Override
    protected CamelContext createCamelContext() throws Exception {
        var context = super.createCamelContext();
        var name = "bt" + System.nanoTime();

        // Producer flow control only on flow.control, so the other probes are not throttled.
        var throttled = new PolicyEntry();
        throttled.setQueue("flow.control");
        throttled.setMemoryLimit(32 * 1024);
        throttled.setProducerFlowControl(true);

        var roomy = new PolicyEntry();
        roomy.setMemoryLimit(64 * 1024 * 1024);
        roomy.setProducerFlowControl(false);

        var policies = new PolicyMap();
        policies.setDefaultEntry(roomy);
        policies.setPolicyEntries(List.of(throttled));

        broker = new BrokerService();
        broker.setBrokerName(name);
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.setAdvisorySupport(false);
        broker.setDestinationPolicy(policies);
        broker.start();

        var amq = new ActiveMQComponent();
        amq.setConnectionFactory(new ActiveMQConnectionFactory("vm://" + name + "?create=false"));
        context.addComponent("activemq", amq);
        return context;
    }

    @Override
    protected void doPostTearDown() throws Exception {
        release.countDown();
        if (broker != null) {
            broker.stop();
        }
    }

    private void hold() throws Exception {
        consumerThreads.add(Thread.currentThread().getName());
        peakInFlight.accumulateAndGet(inFlight.incrementAndGet(), Math::max);
        threeArrived.countDown();
        if (!release.await(10, TimeUnit.SECONDS)) {
            timedOut.incrementAndGet();   // would free a slot and inflate the concurrency ceiling
        }
        inFlight.decrementAndGet();
    }

    @Override
    protected RouteBuilder createRouteBuilder() {
        return new RouteBuilder() {
            @Override
            public void configure() {
                from("activemq:queue:concurrency?concurrentConsumers=3")
                        .process(ex -> hold());

                from("activemq:queue:backlog")
                        .process(ex -> {
                            slowConsumed.incrementAndGet();
                            release.await(10, TimeUnit.SECONDS);
                        });

                from("activemq:queue:flow.control")
                        .process(ex -> release.await(10, TimeUnit.SECONDS));
            }
        };
    }

    @Test
    void concurrentConsumersIsTheConcurrencyBound_andEachGetsItsOwnThread() throws Exception {
        for (int i = 0; i < 6; i++) {
            template.sendBody("activemq:queue:concurrency", "m" + i);
        }
        assertThat(threeArrived.await(5, TimeUnit.SECONDS)).isTrue();
        Thread.sleep(400);

        assertThat(timedOut.get()).describedAs("no hold expired to free a slot early").isZero();
        assertThat(peakInFlight.get())
                .describedAs("six messages waiting and exactly three in flight — a route has no "
                        + "concurrency of its own, the consumer supplies it, and that number is "
                        + "the bound")
                .isEqualTo(3);
        assertThat(consumerThreads)
                .describedAs("one thread per concurrent consumer, and they are Camel's, not the "
                        + "sender's")
                .hasSize(3);

        release.countDown();   // let the consumers drain rather than leaving shutdown to time out
    }

    @Test
    void aSlowConsumerBacksUpInTheBroker_notInTheSender() throws Exception {
        var start = System.nanoTime();
        for (int i = 0; i < 200; i++) {
            template.sendBody("activemq:queue:backlog", "m" + i);
        }
        var elapsedMs = (System.nanoTime() - start) / 1_000_000;

        assertThat(elapsedMs)
                .describedAs("200 sends against a consumer that has processed one and stopped, and "
                        + "none of them blocked or threw. Where seda: would have refused at its "
                        + "in-process limit, the backlog simply moved to the broker.")
                .isLessThan(5000);
        assertThat(slowConsumed.get()).isEqualTo(1);

        release.countDown();
    }

    @Test
    void whenTheBrokerRunsOutOfRoom_theProducerIsTheOneThatWaits() throws Exception {
        var body = "x".repeat(8 * 1024);
        var sender = new Thread(() -> {
            for (int i = 0; i < 50; i++) {
                template.sendBody("activemq:queue:flow.control", body);
            }
        });
        sender.start();

        sender.join(1500);
        assertThat(sender.isAlive())
                .describedAs("with producer flow control the broker stops accepting and the send "
                        + "parks — the broker's backpressure reaches you as a blocked producer, the "
                        + "same shape as blockWhenFull and with the same risk of holding a thread "
                        + "that cannot afford it")
                .isTrue();

        release.countDown();
        sender.join(10000);
    }
}
