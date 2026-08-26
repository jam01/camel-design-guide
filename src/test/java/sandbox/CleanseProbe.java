package sandbox;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The escape hatch, stated as something you could actually put in a codebase.
 * <p>
 * No construct resets an exchange's error state in place. This is what doing it by hand costs and
 * whether it genuinely works — including whether a stage AFTER the reset behaves completely
 * normally, which is the only thing that would make it usable as a pipeline primitive.
 */
public class CleanseProbe extends ProbeSupport {

    private final List<String> mapped = new CopyOnWriteArrayList<>();

    /**
     * Everything {@code prepareExchangeForContinue} does, plus the one flag it leaves behind.
     * Reaching into {@code ExchangeExtension} is the price; there is no public equivalent.
     */
    private static final Processor CLEANSE = ex -> {
        ex.setException(null);
        ex.setRollbackOnly(false);
        ex.setRollbackOnlyLast(false);
        ex.setRouteStop(false);
        ex.getExchangeExtension().setFailureHandled(false);
        ex.getExchangeExtension().setErrorHandlerHandled(null);
        ex.getExchangeExtension().setRedeliveryExhausted(false);
    };

    @Override
    protected RouteBuilder[] createRouteBuilders() {
        var main = new RouteBuilder() {
            @Override
            public void configure() {
                onException(Boom.class)
                        .handled(true)
                        .process(ex -> mapped.add("mapped:" + ex.getIn().getHeader("stage")))
                        .setBody(constant("MAPPED"));

                // Two stages, each a called route that fails, each recovered — with a reset between.
                from("direct:pipeline")
                        .doTry()
                            .to("direct:stage-one-fails")
                        .doCatch(OtherBoom.class)
                        .end()
                        .process(CLEANSE)
                        .setHeader("stage", constant("two"))
                        .process(ex -> {
                            throw new Boom();
                        });

                // The same without the reset, for contrast.
                from("direct:pipeline-no-reset")
                        .doTry()
                            .to("direct:stage-one-fails")
                        .doCatch(OtherBoom.class)
                        .end()
                        .setHeader("stage", constant("two"))
                        .process(ex -> {
                            throw new Boom();
                        });

                // And a reset followed by ANOTHER claiming callee, to check it is not one-shot.
                from("direct:pipeline-twice")
                        .doTry()
                            .to("direct:stage-one-fails")
                        .doCatch(OtherBoom.class)
                        .end()
                        .process(CLEANSE)
                        .doTry()
                            .to("direct:stage-one-fails")
                        .doCatch(OtherBoom.class)
                        .end()
                        .process(CLEANSE)
                        .setHeader("stage", constant("three"))
                        .process(ex -> {
                            throw new Boom();
                        });
            }
        };

        var stages = new RouteBuilder() {
            @Override
            public void configure() {
                from("direct:stage-one-fails")
                        .process(ex -> {
                            throw new OtherBoom();
                        });
            }
        };

        return new RouteBuilder[] { main, stages };
    }

    @Test
    void withoutTheResetTheNextStageCannotBeMapped() {
        var out = template.request("direct:pipeline-no-reset", ex -> ex.getIn().setBody("in"));

        assertThat(mapped).isEmpty();
        assertThat(out.getException()).isInstanceOf(Boom.class);
    }

    @Test
    void withTheResetTheNextStageBehavesEntirelyNormally() {
        var out = template.request("direct:pipeline", ex -> ex.getIn().setBody("in"));

        assertThat(mapped)
                .describedAs("the clause fires for the second stage's failure")
                .containsExactly("mapped:two");
        assertThat(out.getMessage().getBody(String.class)).isEqualTo("MAPPED");
        assertThat(out.getException())
                .describedAs("and the exchange leaves clean, so an HTTP edge renders it — a full "
                        + "repair, not a partial one")
                .isNull();
    }

    @Test
    void andItIsNotOneShot_aPipelineCanResetBetweenEveryStage() {
        var out = template.request("direct:pipeline-twice", ex -> ex.getIn().setBody("in"));

        assertThat(mapped)
                .describedAs("two claimed callees, two resets, and the third stage still maps — so "
                        + "this works as a per-stage primitive rather than a one-time rescue")
                .containsExactly("mapped:three");
        assertThat(out.getMessage().getBody(String.class)).isEqualTo("MAPPED");
        assertThat(out.getException()).isNull();
    }
}
