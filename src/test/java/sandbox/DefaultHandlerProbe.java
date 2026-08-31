package sandbox;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.support.ExchangeHelper;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The difference between declining ownership and an error handler having claimed the failure
 * without saying anything about it.
 * <p>
 * Every route has an error handler whether or not anyone asked for one — with no declaration at
 * all it is a {@code DefaultErrorHandler} holding an empty list of exception policies. That is not
 * the same thing as {@code noErrorHandler()}, and it is not the same thing as a clause that matches
 * but declines to handle. These probes separate the three.
 */
public class DefaultHandlerProbe extends ProbeSupport {

    private final List<String> attempts = new CopyOnWriteArrayList<>();

    @Override
    protected RouteBuilder[] createRouteBuilders() {
        var caller = new RouteBuilder() {
            @Override
            public void configure() {
                onException(Boom.class).handled(true).setBody(constant("CAUGHT-BY-CALLER"));

                from("direct:calls-bare")
                        .to("direct:bare-callee")
                        .setBody(constant("CALLER-CONTINUED"));

                from("direct:calls-declining-clause")
                        .to("direct:declining-clause-callee")
                        .setBody(constant("CALLER-CONTINUED"));

                from("direct:calls-retrying")
                        .to("direct:retrying-callee")
                        .setBody(constant("CALLER-CONTINUED"));

                from("direct:calls-declining-handler")
                        .to("direct:declining-handler-callee")
                        .setBody(constant("CALLER-CONTINUED"));
            }
        };

        // No errorHandler declaration and no clauses: the default handler with nothing registered.
        var bare = new RouteBuilder() {
            @Override
            public void configure() {
                from("direct:bare-callee")
                        .process(ex -> {
                            attempts.add("attempt");
                            throw new Boom();
                        });
            }
        };

        // A clause that matches and does not handle.
        var decliningClause = new RouteBuilder() {
            @Override
            public void configure() {
                onException(Boom.class).setHeader("calleeClauseFired", constant("yes"));

                from("direct:declining-clause-callee")
                        .process(ex -> {
                            throw new Boom();
                        });
            }
        };

        // The default handler, configured. Still no clauses.
        var retrying = new RouteBuilder() {
            @Override
            public void configure() {
                errorHandler(defaultErrorHandler().maximumRedeliveries(2).redeliveryDelay(0));

                from("direct:retrying-callee")
                        .process(ex -> {
                            attempts.add("attempt");
                            throw new Boom();
                        });
            }
        };

        // The only arrangement that declines: a pass-through that never runs the failure path.
        var decliningHandler = new RouteBuilder() {
            @Override
            public void configure() {
                errorHandler(noErrorHandler());

                from("direct:declining-handler-callee")
                        .process(ex -> {
                            throw new Boom();
                        });
            }
        };

        return new RouteBuilder[] { caller, bare, decliningClause, retrying, decliningHandler };
    }

    @Test
    void noClausesAtAll_stillClaimsTheFailure_soTheCallersClauseNeverFires() {
        var out = template.request("direct:calls-bare", ex -> ex.getIn().setBody("in"));

        assertThat(ExchangeHelper.isFailureHandled(out))
                .describedAs("the callee declared nothing at all, and its default error handler "
                        + "claimed the failure anyway — the stamp is the first thing the failure "
                        + "path does, before any clause is consulted. Final state: " + stamps(out))
                .isTrue();
        assertThat(out.getException())
                .describedAs("nothing handled it, so the exception is still live — claimed and "
                        + "unhandled at the same time")
                .isInstanceOf(Boom.class);
        assertThat(out.getMessage().getBody(String.class))
                .describedAs("so the caller's clause is skipped: having no clauses is not the same "
                        + "as declining")
                .isNotEqualTo("CAUGHT-BY-CALLER");
    }

    @Test
    void onlyNoErrorHandler_actuallyDeclines() {
        var out = template.request("direct:calls-declining-handler", ex -> ex.getIn().setBody("in"));

        assertThat(out.getMessage().getBody(String.class))
                .describedAs("a pass-through never reaches the failure path, so nothing is stamped "
                        + "and the caller's clause is the first one to see it")
                .isEqualTo("CAUGHT-BY-CALLER");
        assertThat(out.getException()).isNull();
    }

    @Test
    void aClauseThatDeclinesToHandle_stillClaims_soTheCallersClauseNeverFires() {
        var out = template.request("direct:calls-declining-clause", ex -> ex.getIn().setBody("in"));

        assertThat(out.getMessage().getHeader("calleeClauseFired"))
                .describedAs("the callee's clause did match and run").isEqualTo("yes");
        assertThat(out.getException())
                .describedAs("...and having run, it owns the failure. The exception is still live, "
                        + "but claimed — so the caller's clause is skipped and the failure leaves "
                        + "the caller unmapped.")
                .isInstanceOf(Boom.class);
        assertThat(out.getMessage().getBody(String.class)).isNotEqualTo("CAUGHT-BY-CALLER");
    }

    @Test
    void theDefaultHandlerStillOwnsRedelivery_evenWithNoClausesRegistered() {
        template.request("direct:calls-retrying", ex -> ex.getIn().setBody("in"));

        assertThat(attempts)
                .describedAs("no clause exists, yet the callee retried on its own before the "
                        + "caller ever saw the failure — the handler is doing work whether or not "
                        + "any clause is registered with it")
                .hasSize(3);
    }

    @Test
    void aBareCalleeDoesNotRetry_becauseTheDefaultPolicyIsZeroRedeliveries() {
        template.request("direct:calls-bare", ex -> ex.getIn().setBody("in"));

        assertThat(attempts)
                .describedAs("one attempt, so the default handler is transparent in practice — "
                        + "until somebody configures it")
                .hasSize(1);
    }
}
