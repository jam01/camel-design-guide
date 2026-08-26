package sandbox;

import org.apache.camel.builder.RouteBuilder;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * An independent check of the one variable that decides whether a throw from inside a
 * {@code doCatch} can be mapped by anybody: whether the exchange was already claimed.
 * <p>
 * Written separately from {@code CatchRethrowProbe} because it contradicts a statement that had
 * already been published, and a claim that overturns a published one should have its own witness.
 */
public class CatchEscapeProbe extends ProbeSupport {

    @Override
    protected RouteBuilder[] createRouteBuilders() {
        var caller = new RouteBuilder() {
            @Override
            public void configure() {
                onException(Boom.class).handled(true).setBody(constant("MAPPED"));

                from("direct:calls-inline").to("direct:rethrows-after-inline");
                from("direct:calls-claimed").to("direct:rethrows-after-claimed");
            }
        };

        var routes = new RouteBuilder() {
            @Override
            public void configure() {
                // The thing that fails inside doTry is an inline step: nothing claims it.
                from("direct:rethrows-after-inline")
                        .doTry()
                            .process(ex -> {
                                throw new OtherBoom();
                            })
                        .doCatch(OtherBoom.class)
                            .process(ex -> {
                                throw new Boom();
                            })
                        .end();

                // Identical, except the thing that fails is a CALLED ROUTE, which claims.
                from("direct:rethrows-after-claimed")
                        .doTry()
                            .to("direct:claims-it")
                        .doCatch(OtherBoom.class)
                            .process(ex -> {
                                throw new Boom();
                            })
                        .end();
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

        return new RouteBuilder[] { caller, routes, claiming };
    }

    @Test
    void anUnclaimedRethrowFromACatchIsMappedByTheCaller() {
        var out = template.request("direct:calls-inline", ex -> ex.getIn().setBody("in"));

        assertThat(out.getMessage().getBody(String.class))
                .describedAs("the failing step was inline, so nothing claimed the exchange. The "
                        + "rethrow escapes the catch unclaimed and the caller's clause maps it — "
                        + "delegating out of a doCatch does work, to your caller.")
                .isEqualTo("MAPPED");
        assertThat(out.getException()).isNull();
    }

    @Test
    void theSameRethrowIsNotMappedOnceSomethingHasClaimed() {
        var out = template.request("direct:calls-claimed", ex -> ex.getIn().setBody("in"));

        assertThat(out.getMessage().getBody(String.class))
                .describedAs("identical route, one difference: the thing inside doTry is a called "
                        + "route, which claimed the failure. The exchange is finished as far as "
                        + "every later handler is concerned, so the caller's clause is skipped and "
                        + "the rethrow reaches nobody.")
                .isNotEqualTo("MAPPED");
        assertThat(out.getException())
                .describedAs("it leaves the route failed, which at an HTTP edge means an empty body")
                .isNotNull();
    }
}
