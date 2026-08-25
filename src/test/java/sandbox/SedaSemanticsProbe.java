package sandbox;

import org.apache.camel.builder.RouteBuilder;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The two things about an in-memory queue that a broker answers differently: whether a consumer's
 * failure can reach the sender, and what becomes of the message when it fails.
 * <p>
 * A broker answers both with the session — a transacted consumer un-acknowledges and the message
 * comes back. {@code seda:} has no session and no acknowledgement, so the answers are its own, and
 * the first of them is a producer option rather than a property of the queue.
 */
public class SedaSemanticsProbe extends ProbeSupport {

    private final List<String> deliveries = new CopyOnWriteArrayList<>();
    private final List<String> parked = new CopyOnWriteArrayList<>();
    private final CountDownLatch consumed = new CountDownLatch(1);
    private final CountDownLatch dead = new CountDownLatch(1);

    @Override
    protected RouteBuilder[] createRouteBuilders() {
        var sender = new RouteBuilder() {
            @Override
            public void configure() {
                onException(Boom.class).handled(true).setBody(constant("CAUGHT-BY-SENDER"));

                from("direct:never")
                        .to("seda:claiming?waitForTaskToComplete=Never")
                        .setBody(constant("SENDER-CONTINUED"));

                from("direct:always")
                        .to("seda:claiming?waitForTaskToComplete=Always")
                        .setBody(constant("SENDER-CONTINUED"));

                from("direct:always-declining")
                        .to("seda:declining?waitForTaskToComplete=Always")
                        .setBody(constant("SENDER-CONTINUED"));
            }
        };

        var claiming = new RouteBuilder() {
            @Override
            public void configure() {
                from("seda:claiming?pollTimeout=100")
                        .process(ex -> {
                            deliveries.add("d");
                            consumed.countDown();
                            throw new Boom();
                        });

                // The only place a failed in-memory message can go other than the floor.
                from("seda:with-dlq?pollTimeout=100")
                        .errorHandler(deadLetterChannel("seda:parked"))
                        .process(ex -> {
                            throw new Boom();
                        });

                from("seda:parked?pollTimeout=100")
                        .process(ex -> {
                            parked.add(ex.getIn().getBody(String.class));
                            dead.countDown();
                        });
            }
        };

        var declining = new RouteBuilder() {
            @Override
            public void configure() {
                errorHandler(noErrorHandler());

                from("seda:declining?pollTimeout=100")
                        .process(ex -> {
                            throw new Boom();
                        });
            }
        };

        return new RouteBuilder[] { sender, claiming, declining };
    }

    @Test
    void withoutWaiting_theSenderNeverLearns() throws Exception {
        var out = template.request("direct:never", ex -> ex.getIn().setBody("in"));

        assertThat(consumed.await(5, TimeUnit.SECONDS)).isTrue();

        assertThat(out.getMessage().getBody(String.class))
                .describedAs("the send returned before the consumer ran, so the sender finished its "
                        + "own route none the wiser")
                .isEqualTo("SENDER-CONTINUED");
        assertThat(out.getException()).isNull();
    }

    @Test
    void evenWhenTheSenderWaits_theConsumerHasAlreadyClaimedTheFailure() {
        var out = template.request("direct:always", ex -> ex.getIn().setBody("in"));

        assertThat(out.getException())
                .describedAs("waitForTaskToComplete=Always does bring the failure back — so whether "
                        + "a queue hop hides errors is a producer option, not a property of seda:")
                .isInstanceOf(Boom.class);
        assertThat(out.getMessage().getBody(String.class))
                .describedAs("but the consumer's own error handler claimed it first, so the "
                        + "sender's clause never fired and the sender was merely stopped — exactly "
                        + "the ownership rule that governs a direct: callee")
                .isEqualTo("in");
    }

    @Test
    void aDecliningConsumerLetsTheSendersClauseFire() {
        var out = template.request("direct:always-declining", ex -> ex.getIn().setBody("in"));

        assertThat(out.getMessage().getBody(String.class))
                .describedAs("noErrorHandler() on the consumer plus a waiting producer is the only "
                        + "arrangement where a sender can map a queue consumer's failure")
                .isEqualTo("CAUGHT-BY-SENDER");
    }

    @Test
    void aFailedMessageIsNotRedelivered_itIsSimplyGone() throws Exception {
        template.sendBody("seda:claiming", "in");

        assertThat(consumed.await(5, TimeUnit.SECONDS)).isTrue();
        Thread.sleep(300);

        assertThat(deliveries)
                .describedAs("no session and no acknowledgement, so there is nothing to "
                        + "un-acknowledge: one delivery and the message is gone. The same loss as a "
                        + "plain broker consumer, without the transacted option that fixes it.")
                .hasSize(1);
    }

    @Test
    void aDeadLetterChannelIsTheOnlyPlaceAFailedMessageCanGo() throws Exception {
        template.sendBody("seda:with-dlq", "payload");

        assertThat(dead.await(5, TimeUnit.SECONDS))
                .describedAs("an in-memory queue has no dead-letter queue of its own — the error "
                        + "handler is where one has to be built, and it is still only memory")
                .isTrue();
        assertThat(parked).containsExactly("payload");
    }
}
