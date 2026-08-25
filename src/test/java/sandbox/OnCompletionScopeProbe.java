package sandbox;

import org.apache.camel.builder.RouteBuilder;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Whether {@code onCompletion} is per route or per builder, how many fire, and when.
 * <p>
 * It matters for compensation and for post-commit triggers: a hook that fires once per exchange is
 * a different tool from one that fires for every route the exchange passed through, and a hook that
 * is quietly replaced by a nearer one is a different tool again.
 */
public class OnCompletionScopeProbe extends ProbeSupport {

    private final List<String> fired = new CopyOnWriteArrayList<>();

    @Override
    protected RouteBuilder createRouteBuilder() {
        return new RouteBuilder() {
            @Override
            public void configure() {
                // Builder-scoped: written in configure(), attached to no particular route.
                onCompletion().process(ex -> fired.add("builder"));

                from("direct:caller").routeId("caller")
                        .onCompletion().process(ex -> fired.add("route:caller")).end()
                        .to("direct:callee")
                        .process(ex -> fired.add("caller-body-end"));

                from("direct:callee").routeId("callee")
                        .onCompletion().process(ex -> fired.add("route:callee")).end()
                        .process(ex -> fired.add("callee-body"));

                // Declares no hook of its own.
                from("direct:plain").routeId("plain")
                        .process(ex -> fired.add("plain-body"));
            }
        };
    }

    @Test
    void aRouteScopedHookFiresForEveryRouteTheExchangeVisited_innermostFirst() {
        template.request("direct:caller", ex -> ex.getIn().setBody("in"));

        assertThat(fired)
                .describedAs("the callee was never invoked directly, yet its own hook still ran — "
                        + "and the hooks unwind inside-out")
                .containsSubsequence("route:callee", "route:caller");
    }

    @Test
    void aRouteScopedHookReplacesTheBuilderScopedOne_ratherThanAddingToIt() {
        template.request("direct:caller", ex -> ex.getIn().setBody("in"));

        assertThat(fired)
                .describedAs("both routes declared their own, so the builder-scoped hook never ran "
                        + "at all — a nearer onCompletion silently takes the place of the shared "
                        + "one instead of running alongside it")
                .doesNotContain("builder");
    }

    @Test
    void aRouteWithoutItsOwnHook_stillGetsTheBuilderScopedOne() {
        template.request("direct:plain", ex -> ex.getIn().setBody("in"));

        assertThat(fired).containsExactly("plain-body", "builder");
    }

    @Test
    void everyHookRunsAfterTheWholeRoutingIsDone_notAtEachRoutesOwnEnd() {
        template.request("direct:caller", ex -> ex.getIn().setBody("in"));

        assertThat(fired)
                .describedAs("the caller's last step ran before any hook — they hang off the unit "
                        + "of work, which ends once, not off each route as it finishes")
                .containsSubsequence("callee-body", "caller-body-end", "route:callee");
    }
}
