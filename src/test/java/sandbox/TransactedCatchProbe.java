package sandbox;

import org.apache.camel.builder.RouteBuilder;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The one way a caller can still recover from a transacted callee.
 * <p>
 * A transacted route cannot decline ownership, so the caller's {@code onException} never fires.
 * {@code doTry}/{@code doCatch} walks its own clause list without consulting those gates, so it
 * should still reach the catch — provided the exception is intact and the catch matches what the
 * transaction manager actually left on the exchange.
 */
public class TransactedCatchProbe extends ProbeSupport {

    private final List<String> events = new CopyOnWriteArrayList<>();

    @Override
    protected RouteBuilder[] createRouteBuilders() {
        var caller = new RouteBuilder() {
            @Override
            public void configure() {
                from("direct:try-around-tx")
                        .doTry()
                            .to("direct:tx-callee")
                        .doCatch(Boom.class)
                            .process(ex -> events.add("caught"))
                            .setBody(constant("RECOVERED"))
                        .end()
                        .process(ex -> events.add("carried-on"));
            }
        };

        var callee = new RouteBuilder() {
            @Override
            public void configure() {
                from("direct:tx-callee")
                        .transacted()
                        .to(insertRow("row"))
                        .process(ex -> {
                            throw new Boom();
                        });
            }
        };

        return new RouteBuilder[] { caller, callee };
    }

    @Test
    void doCatchReachesAFailureFromATransactedCallee_andTheRollbackStillHappens() throws Exception {
        var out = template.request("direct:try-around-tx", ex -> ex.getIn().setBody("in"));

        assertThat(events)
                .describedAs("doCatch is reachable on a claimed exchange, and the caller resumes "
                        + "after end() — the recovery a transacted callee otherwise forbids")
                .containsExactly("caught", "carried-on");
        assertThat(out.getMessage().getBody(String.class)).isEqualTo("RECOVERED");
        assertThat(rows())
                .describedAs("the callee's transaction still rolled back — catching the failure "
                        + "upstream does not resurrect its writes")
                .isZero();
        assertThat(out.getException()).isNull();
    }
}
