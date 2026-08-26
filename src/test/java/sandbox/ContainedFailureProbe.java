package sandbox;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.support.ExchangeHelper;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Two questions about how much room the design actually has.
 * <p>
 * First: can {@code continued(true)} fire twice on one exchange, which is what CAMEL-5139 said it
 * could not. Second, and the one that matters for laying out a request/reply app: if a stage keeps
 * its failure entirely to itself — catching it in its own {@code doTry} rather than letting any
 * clause see it — does the exchange come back <em>unclaimed</em>, leaving later stages and the edge
 * free to translate their own failures?
 */
public class ContainedFailureProbe extends ProbeSupport {

    private final List<String> steps = new CopyOnWriteArrayList<>();
    private final List<String> mapped = new CopyOnWriteArrayList<>();

    @Override
    protected RouteBuilder[] createRouteBuilders() {
        var main = new RouteBuilder() {
            @Override
            public void configure() {
                onException(Boom.class).continued(true);

                onException(OtherBoom.class)
                        .handled(true)
                        .process(ex -> mapped.add("edge-mapped"))
                        .setBody(constant("EDGE-MAPPED"));

                from("direct:continued-twice")
                        .process(ex -> {
                            throw new Boom();
                        })
                        .process(ex -> steps.add("after-first"))
                        .process(ex -> {
                            throw new Boom();
                        })
                        .process(ex -> steps.add("after-second"));

                // A transacted stage that never lets a failure escape as a failure: it catches its
                // own, records a verdict, and returns normally.
                from("direct:caller")
                        .to("direct:contained-stage")
                        .process(ex -> steps.add("caller-resumed verdict="
                                + ex.getIn().getHeader("stageVerdict")
                                + " claimed=" + ExchangeHelper.isFailureHandled(ex)))
                        .process(ex -> {
                            throw new OtherBoom();
                        });
            }
        };

        var stage = new RouteBuilder() {
            @Override
            public void configure() {
                from("direct:contained-stage")
                        .transacted()
                        .to(insertRow("work"))
                        .doTry()
                            .process(ex -> {
                                throw new OtherBoom();
                            })
                        .doCatch(OtherBoom.class)
                            .setHeader("stageVerdict", constant("failed"))
                        .end();
            }
        };

        return new RouteBuilder[] { main, stage };
    }

    @Test
    void continuedCanFireTwiceOnOneExchange() {
        template.request("direct:continued-twice", ex -> ex.getIn().setBody("in"));

        assertThat(steps)
                .describedAs("continued(true) clears the claim every time it fires, so it is not "
                        + "one-shot — the CAMEL-5139 complaint is fixed. That is the whole reason "
                        + "the clause system copes and doCatch does not: the reset was attached to "
                        + "clauses.")
                .containsExactly("after-first", "after-second");
    }

    @Test
    void swallowingAFailureLeavesTheExchangeCleanAndTheWorkCOMMITTED() throws Exception {
        var out = template.request("direct:caller", ex -> ex.getIn().setBody("in"));

        assertThat(steps)
                .describedAs("no clause ever saw the stage's failure, so nothing claimed: the "
                        + "caller resumes with a verdict to read and an unclaimed exchange")
                .containsExactly("caller-resumed verdict=failed claimed=false");
        assertThat(mapped)
                .describedAs("...and a LATER failure in the caller is still fully mappable, which "
                        + "is the thing a claim would have taken away")
                .containsExactly("edge-mapped");
        assertThat(out.getMessage().getBody(String.class)).isEqualTo("EDGE-MAPPED");
        assertThat(out.getException()).isNull();
        assertThat(rows())
                .describedAs("and here is the price: the stage failed, nothing reached the "
                        + "transaction boundary, so the work COMMITTED. Do not reach for this.")
                .isEqualTo(1);
    }
}
