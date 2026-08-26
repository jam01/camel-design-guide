package sandbox;

import org.apache.camel.RoutesBuilder;
import org.apache.camel.builder.RouteBuilder;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Whether a callee that declines lets the caller's mapping work end to end.
 * <p>
 * {@link DefaultHandlerProbe} and {@link TransactedDeclineProbe} establish that {@code noErrorHandler()}
 * is the only way for a route to decline ownership of its failures, and that a transacted route cannot.
 * What was never measured is the consequence a convention would rest on: that a declined failure reaches
 * the caller's clause, is mapped by it, and leaves the exchange clean enough for an HTTP edge to render
 * the body — the edge only reads the body when no exception is present.
 * <p>
 * The declining side sets {@code noErrorHandler()} at <em>builder</em> scope, one line for every route in
 * it, because that is what makes the convention cheap enough to adopt.
 */
public class DecliningCalleeProbe extends ProbeSupport {

    private final List<String> mapped = new CopyOnWriteArrayList<>();
    private final List<String> seen = new CopyOnWriteArrayList<>();

    public static class Mapped extends RuntimeException {
    }

    @Override
    protected RoutesBuilder[] createRouteBuilders() {
        // The helper builder: every route in it declines.
        var helpers = new RouteBuilder() {
            @Override
            public void configure() {
                errorHandler(noErrorHandler());

                from("direct:declining-callee")
                        .to(insertRow("callee"))
                        .process(ex -> {
                            throw new Mapped();
                        });

                from("direct:declining-transacted-callee")
                        .transacted()
                        .to(insertRow("tx-callee"))
                        .process(ex -> {
                            throw new Mapped();
                        });
            }
        };

        // The edge builder: owns the mapping.
        var edge = new RouteBuilder() {
            @Override
            public void configure() {
                onException(Mapped.class)
                        .handled(true)
                        .process(ex -> mapped.add("mapped"))
                        .setBody(constant("{\"error\":\"mapped\"}"));

                from("direct:edge-calls-decliner")
                        .to("direct:declining-callee")
                        .process(ex -> seen.add("resumed"));

                from("direct:edge-calls-transacted-decliner")
                        .to("direct:declining-transacted-callee")
                        .process(ex -> seen.add("resumed"));
            }
        };

        return new RoutesBuilder[] { helpers, edge };
    }

    @Test
    void aDecliningCalleeLetsTheCallersClauseMapTheFailure() {
        var out = template.request("direct:edge-calls-decliner", ex -> ex.getIn().setBody("in"));

        assertThat(mapped)
                .describedAs("the caller's clause fires for a failure raised one direct: hop away, "
                        + "because the callee declined instead of claiming")
                .containsExactly("mapped");
        assertThat(out.getMessage().getBody(String.class))
                .describedAs("and the mapped body is what comes out")
                .isEqualTo("{\"error\":\"mapped\"}");
        assertThat(out.getException())
                .describedAs("and the exchange leaves clean, which is the half that matters at an HTTP "
                        + "edge: the body is only read when no exception is present, so this is the "
                        + "difference between the client seeing the mapped JSON and seeing an empty "
                        + "text/plain 500")
                .isNull();
    }

    @Test
    void theCallerStillDoesNotResumeAfterTheCall() {
        template.request("direct:edge-calls-decliner", ex -> ex.getIn().setBody("in"));

        assertThat(seen)
                .describedAs("declining changes who maps the failure, not whether routing continues: "
                        + "handled(true) still stops the caller at the point of failure, so the step "
                        + "after the call does not run")
                .isEmpty();
    }

    @Test
    void builderScopedDecliningDoesNotSurviveTransacted() {
        var out = template.request("direct:edge-calls-transacted-decliner", ex -> ex.getIn().setBody("in"));

        assertThat(mapped)
                .describedAs("a transacted route claims its failures whatever the builder says, so the "
                        + "caller's clause never fires -- the one place the convention cannot reach, "
                        + "and the reason a transaction boundary is always an error boundary")
                .isEmpty();
        assertThat(out.getException())
                .describedAs("and the failure comes back live, which at an HTTP edge is the muted 500. "
                        + "Note it comes back WRAPPED in a RuntimeCamelException by the transaction "
                        + "handler, so a caller's clause on the original type would not have matched it "
                        + "even if the claim had not already skipped the clauses.")
                .isNotNull()
                .hasRootCauseInstanceOf(Mapped.class);
    }
}
