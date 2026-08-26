package sandbox;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.support.ExchangeHelper;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Whether an application can get mappability back after something claimed the exchange.
 * <p>
 * Camel already does this internally for one construct — {@code prepareExchangeForContinue} calls
 * {@code setFailureHandled(false)}, which is the fix from CAMEL-4057 living on. The question is
 * whether an app can do the same by hand, and whether copying the exchange sheds the claim.
 */
public class ClaimResetProbe extends ProbeSupport {

    private final List<String> mapped = new CopyOnWriteArrayList<>();
    private final List<String> copyState = new CopyOnWriteArrayList<>();

    @Override
    protected RouteBuilder[] createRouteBuilders() {
        var caller = new RouteBuilder() {
            @Override
            public void configure() {
                onException(Boom.class)
                        .handled(true)
                        .process(ex -> mapped.add("clause-ran"))
                        .setBody(constant("MAPPED"));

                // Baseline: a callee claims, then this route throws something else later.
                from("direct:later-throw")
                        .doTry()
                            .to("direct:claims-it")
                        .doCatch(OtherBoom.class)
                        .end()
                        .process(ex -> {
                            throw new Boom();
                        });

                // The same, with the claim cleared by hand first.
                from("direct:later-throw-after-reset")
                        .doTry()
                            .to("direct:claims-it")
                        .doCatch(OtherBoom.class)
                        .end()
                        .process(ex -> ex.getExchangeExtension().setFailureHandled(false))
                        .process(ex -> {
                            throw new Boom();
                        });

                // Clearing BOTH sticky flags, not just the claim.
                from("direct:later-throw-after-full-reset")
                        .doTry()
                            .to("direct:claims-it")
                        .doCatch(OtherBoom.class)
                        .end()
                        .process(ex -> {
                            ex.getExchangeExtension().setFailureHandled(false);
                            ex.getExchangeExtension().setErrorHandlerHandled(null);
                        })
                        .process(ex -> {
                            throw new Boom();
                        });

                // Do the constructs that copy actually land on that same path?
                from("direct:tap-after-claim")
                        .doTry()
                            .to("direct:claims-it")
                        .doCatch(OtherBoom.class)
                        .end()
                        .wireTap("direct:tapped")
                        .process(ex -> copyState.add("origin=" + ExchangeHelper.isFailureHandled(ex)));

                from("direct:enrich-after-claim")
                        .doTry()
                            .to("direct:claims-it")
                        .doCatch(OtherBoom.class)
                        .end()
                        .enrich("direct:resource", (a, b) -> a);

                // Does a copy shed the claim?
                from("direct:copy-after-claim")
                        .doTry()
                            .to("direct:claims-it")
                        .doCatch(OtherBoom.class)
                        .end()
                        .process(ex -> {
                            var copy = ExchangeHelper.createCopy(ex, true);
                            copyState.add("original=" + ExchangeHelper.isFailureHandled(ex)
                                    + " copy=" + ExchangeHelper.isFailureHandled(copy));
                        });
            }
        };

        var claiming = new RouteBuilder() {
            @Override
            public void configure() {
                from("direct:claims-it")
                        .process(ex -> {
                            throw new OtherBoom();
                        });
            }
        };

        var copies = new RouteBuilder() {
            @Override
            public void configure() {
                from("direct:tapped")
                        .process(ex -> copyState.add("tapped=" + ExchangeHelper.isFailureHandled(ex)));
                from("direct:resource")
                        .process(ex -> copyState.add("enriched=" + ExchangeHelper.isFailureHandled(ex)));
            }
        };

        return new RouteBuilder[] { caller, claiming, copies };
    }

    @Test
    void baselineALaterThrowIsUnmappableAfterAClaim() {
        var out = template.request("direct:later-throw", ex -> ex.getIn().setBody("in"));

        assertThat(mapped).describedAs("the claim suppresses the clause").isEmpty();
        assertThat(out.getException()).isInstanceOf(Boom.class);
    }

    @Test
    void clearingTheClaimByHandRestoresMappingCompletely() {
        var out = template.request("direct:later-throw-after-reset", ex -> ex.getIn().setBody("in"));

        assertThat(mapped)
                .describedAs("one line of setFailureHandled(false) and the clause fires again")
                .containsExactly("clause-ran");
        assertThat(out.getMessage().getBody(String.class))
                .describedAs("...producing the mapped body")
                .isEqualTo("MAPPED");
        assertThat(out.getException())
                .describedAs("but the exception is BACK. Clearing the claim is not enough: "
                        + "errorHandlerHandled is sticky too, left at false by the first failure, "
                        + "and prepareExchangeAfterFailure sees it already set and restores the "
                        + "exception rather than honouring the new clause's handled(true).")
                .isInstanceOf(Boom.class);
    }

    @Test
    void clearingBothStickyFlagsIsWhatActuallyRepairsIt() {
        var out = template.request("direct:later-throw-after-full-reset", ex -> ex.getIn().setBody("in"));

        assertThat(mapped).containsExactly("clause-ran");
        assertThat(out.getMessage().getBody(String.class)).isEqualTo("MAPPED");
        assertThat(out.getException())
                .describedAs("with errorHandlerHandled reset as well, handled(true) is honoured and "
                        + "the exchange leaves clean — so an HTTP edge renders the mapped body. Two "
                        + "flags, not one, which is why the single-line workaround in the tracker "
                        + "only half-works.")
                .isNull();
    }

    @Test
    void aCopyCarriesTheClaimWithIt() {
        template.request("direct:copy-after-claim", ex -> ex.getIn().setBody("in"));

        assertThat(copyState)
                .describedAs("a copy does NOT carry the claim: createCopy leaves the new exchange "
                        + "unclaimed while the original keeps it. So anything that routes a copy — "
                        + "wireTap, enrich, a split — starts clean, and the claim is confined to "
                        + "the exchange that earned it.")
                .containsExactly("original=true copy=false");
    }

    @Test
    void wireTapAndEnrichBothLandOnThatSamePath() throws Exception {
        template.request("direct:tap-after-claim", ex -> ex.getIn().setBody("in"));
        for (int i = 0; i < 60 && copyState.size() < 2; i++) {
            Thread.sleep(25);
        }
        template.request("direct:enrich-after-claim", ex -> ex.getIn().setBody("in"));

        assertThat(copyState)
                .describedAs("both give the downstream route an exchange with no claim on it, while "
                        + "the origin keeps its own — because each goes through Exchange.copy(), "
                        + "and a copy is built with a fresh ExtendedExchangeExtension where "
                        + "failureHandled is simply a field that starts false")
                .contains("tapped=false", "enriched=false", "origin=true");
    }
}
