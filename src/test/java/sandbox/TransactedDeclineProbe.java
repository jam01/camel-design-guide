package sandbox;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.support.ExchangeHelper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Whether a transacted stage can decline ownership of its failures the way an ordinary one can.
 * <p>
 * {@code .transacted()} replaces the route's error handler with the transaction one, and
 * {@code noErrorHandler()} is a pass-through that does not support transactions. The two cannot
 * both be in force, so one of them loses — this establishes which.
 */
public class TransactedDeclineProbe extends ProbeSupport {

    @Override
    protected RouteBuilder[] createRouteBuilders() {
        var caller = new RouteBuilder() {
            @Override
            public void configure() {
                onException(Boom.class).handled(true).setBody(constant("CAUGHT-BY-CALLER"));

                from("direct:calls-declining-tx").to("direct:declining-tx-callee");
                from("direct:calls-bare-tx").to("direct:bare-tx-callee");
            }
        };

        // Asks to decline, and is transacted anyway.
        var decliningTx = new RouteBuilder() {
            @Override
            public void configure() {
                errorHandler(noErrorHandler());

                from("direct:declining-tx-callee")
                        .transacted()
                        .to(insertRow("row"))
                        .process(ex -> {
                            throw new Boom();
                        });
            }
        };

        // Transacted, declaring nothing.
        var bareTx = new RouteBuilder() {
            @Override
            public void configure() {
                from("direct:bare-tx-callee")
                        .transacted()
                        .to(insertRow("row"))
                        .process(ex -> {
                            throw new Boom();
                        });
            }
        };

        return new RouteBuilder[] { caller, decliningTx, bareTx };
    }

    @Test
    void aTransactedRouteCannotDecline_noErrorHandlerIsOverridden() throws Exception {
        var out = template.request("direct:calls-declining-tx", ex -> ex.getIn().setBody("in"));

        assertThat(ExchangeHelper.isFailureHandled(out))
                .describedAs("the transaction error handler took the route regardless, and it "
                        + "claims like any other — a transacted stage has no way to decline")
                .isTrue();
        assertThat(out.getMessage().getBody(String.class))
                .describedAs("so the caller's clause never fires")
                .isNotEqualTo("CAUGHT-BY-CALLER");
        assertThat(rows()).describedAs("and the transaction is real, so the row rolled back").isZero();
    }

    @Test
    void aBareTransactedRoute_claimsAndRollsBack() throws Exception {
        var out = template.request("direct:calls-bare-tx", ex -> ex.getIn().setBody("in"));

        assertThat(ExchangeHelper.isFailureHandled(out)).isTrue();
        assertThat(out.getException())
                .describedAs("claimed but unhandled, exactly as an untransacted bare route "
                        + "(Spring's manager wraps it to force the rollback, so assert the cause)")
                .hasRootCauseInstanceOf(Boom.class);
        assertThat(rows()).isZero();
    }
}
