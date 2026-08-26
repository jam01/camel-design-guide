package sandbox;

import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Whether {@link ContinueExchangeProcessor} actually buys back what a claim took, when used where
 * it is meant to be used: inside a {@code doCatch}, recovering from a transacted callee that failed
 * and rolled back properly.
 */
public class ContinueExchangeProbe extends ProbeSupport {

    private static final ContinueExchangeProcessor CONTINUE = new ContinueExchangeProcessor();

    private final List<String> mapped = new CopyOnWriteArrayList<>();

    @Override
    protected RouteBuilder[] createRouteBuilders() {
        var caller = new RouteBuilder() {
            @Override
            public void configure() {
                onException(Boom.class)
                        .handled(true)
                        .process(ex -> mapped.add("edge-mapped"))
                        .setBody(constant("EDGE-MAPPED"));

                // The shape that has no good answer: the stage throws so its transaction rolls
                // back, the caller catches to carry on, and a later failure needs mapping.
                from("direct:without-continue")
                        .doTry()
                            .to("direct:tx-stage")
                        .doCatch(OtherBoom.class)
                        .end()
                        .process(ex -> {
                            throw new Boom();
                        });

                from("direct:with-continue")
                        .doTry()
                            .to("direct:tx-stage")
                        .doCatch(OtherBoom.class)
                            .process(CONTINUE)
                        .end()
                        .process(ex -> {
                            throw new Boom();
                        });
            }
        };

        var stage = new RouteBuilder() {
            @Override
            public void configure() {
                from("direct:tx-stage")
                        .transacted()
                        .to(insertRow("work"))
                        .process(ex -> {
                            throw new OtherBoom();
                        });
            }
        };

        return new RouteBuilder[] { caller, stage };
    }

    @Test
    void withoutIt_theLaterFailureCannotBeMapped() throws Exception {
        var out = template.request("direct:without-continue", ex -> ex.getIn().setBody("in"));

        assertThat(rows()).describedAs("the stage did roll back, which is the point").isZero();
        assertThat(mapped).isEmpty();
        assertThat(out.getException()).isInstanceOf(Boom.class);
    }

    @Test
    void withIt_theRollbackStillHappenedAndTheLaterFailureMapsNormally() throws Exception {
        var out = template.request("direct:with-continue", ex -> ex.getIn().setBody("in"));

        assertThat(rows())
                .describedAs("the transaction still rolled back — the stage threw, which is what "
                        + "makes it roll back, and the reset happens afterwards in the caller")
                .isZero();
        assertThat(mapped)
                .describedAs("and the caller's later failure is mappable again")
                .containsExactly("edge-mapped");
        assertThat(out.getMessage().getBody(String.class)).isEqualTo("EDGE-MAPPED");
        assertThat(out.getException()).isNull();
    }

    @Test
    void theCauseIsStillReadableAfterwards() {
        template.request("direct:with-continue", ex -> ex.getIn().setBody("in"));

        assertThat(mapped).containsExactly("edge-mapped");
    }

    @Test
    void itDoesNotResurrectTheStagesWork() throws Exception {
        template.request("direct:with-continue", ex -> ex.getIn().setBody("in"));

        assertThat(rows())
                .describedAs("resetting error state is not a second chance at the transaction — "
                        + "the rollback already happened and nothing here undoes it")
                .isZero();
    }
}
