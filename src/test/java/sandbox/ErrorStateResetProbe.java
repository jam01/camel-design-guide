package sandbox;

import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.support.ExchangeHelper;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Which constructs reset which pieces of error state, side by side.
 * <p>
 * Two flags decide whether a later failure can still be mapped, and they behave differently.
 * {@code errorHandlerHandled} matters in three states, not two: <em>unset</em> means no handler has
 * ruled on this exchange, while <em>set to false</em> means one has and said "not handled" — and
 * the second is what makes {@code prepareExchangeAfterFailure} restore an exception over the top of
 * a later clause's {@code handled(true)}.
 */
public class ErrorStateResetProbe extends ProbeSupport {

    private final List<String> state = new CopyOnWriteArrayList<>();

    /** The distinction {@code stamps()} flattens: unset is not the same as set-to-false. */
    private static String errorState(Exchange ex) {
        var xt = ex.getExchangeExtension();
        String handled = !xt.isErrorHandlerHandledSet() ? "unset"
                : String.valueOf(xt.isErrorHandlerHandled());
        return "claimed=" + ExchangeHelper.isFailureHandled(ex)
                + " handled=" + handled
                + " exception=" + (ex.getException() != null)
                + " rollbackMark=" + ex.isRollbackOnly();
    }

    @Override
    protected RouteBuilder[] createRouteBuilders() {
        var main = new RouteBuilder() {
            @Override
            public void configure() {
                onException(Boom.class).continued(true);

                // doCatch: routing continues, but what did it reset?
                from("direct:after-do-catch")
                        .doTry()
                            .to("direct:claims-it")
                        .doCatch(OtherBoom.class)
                        .end()
                        .process(ex -> state.add("doCatch: " + errorState(ex)));

                // continued(true): Camel's own reset, from the CAMEL-4057 fix.
                from("direct:after-continued")
                        .process(ex -> {
                            throw new Boom();
                        })
                        .process(ex -> state.add("continued: " + errorState(ex)));

                // a copy taken after a claim
                from("direct:after-copy")
                        .doTry()
                            .to("direct:claims-it")
                        .doCatch(OtherBoom.class)
                        .end()
                        .process(ex -> state.add(
                                "copy: " + errorState(ExchangeHelper.createCopy(ex, true))));
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

        return new RouteBuilder[] { main, claiming };
    }

    @Test
    void doCatchResetsTheExceptionAndNothingElse() {
        template.request("direct:after-do-catch", ex -> ex.getIn().setBody("in"));

        assertThat(state)
                .describedAs("routing continues because the exception is gone, and BOTH flags "
                        + "survive — handled is not merely absent, it is pinned to false by the "
                        + "callee's handler, which is the state that later overrides a clause")
                .containsExactly("doCatch: claimed=true handled=false exception=false rollbackMark=false");
    }

    @Test
    void continuedIsAFullResetButOnlyForAFailureItCanActuallySee() {
        template.request("direct:after-continued", ex -> ex.getIn().setBody("in"));

        assertThat(state)
                .describedAs("on a failure it handles itself, continued(true) leaves the exchange "
                        + "completely clean — this is the CAMEL-4057 fix. But it is a CLAUSE, and a "
                        + "claimed exchange never reaches a clause, so it can never repair state "
                        + "inherited from an earlier failure. A reset that helps would have to be a "
                        + "step, not a clause.")
                .containsExactly("continued: claimed=false handled=unset exception=false rollbackMark=false");
    }

    @Test
    void aCopyShedsTheClaimButKeepsHandledPinned() {
        template.request("direct:after-copy", ex -> ex.getIn().setBody("in"));

        assertThat(state)
                .describedAs("the copy constructor propagates seven pieces of error state by hand "
                        + "— errorHandlerHandled among them — and simply does not list "
                        + "failureHandled. So a copy sheds the claim and keeps handled pinned "
                        + "false: the two flags that jointly decide mappability are copied "
                        + "inconsistently, and no single construct clears both.")
                .containsExactly("copy: claimed=false handled=false exception=false rollbackMark=false");
    }
}
