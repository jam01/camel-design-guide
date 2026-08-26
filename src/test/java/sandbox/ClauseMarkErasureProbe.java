package sandbox;

import org.apache.camel.builder.RouteBuilder;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Which clauses erase a rollback mark set by the step that threw — and where a mark has to be set to
 * survive one.
 * <p>
 * {@code RedeliveryErrorHandler:1454-1462} clears {@code rollbackOnly} under
 * {@code isDeadLetterChannel || shouldHandle || shouldContinue}, before any branch-specific work.
 * So the erasure belongs to <em>being handled or continued at all</em>, not to
 * {@code continued(true)}: {@code prepareExchangeForContinue}'s own {@code setRollbackOnly(false)}
 * at {@code :1241} is a second, redundant clear on a flag that is already false.
 */
public class ClauseMarkErasureProbe extends ProbeSupport {

    public static class ThirdBoom extends RuntimeException {
    }

    private final List<String> trace = new CopyOnWriteArrayList<>();

    @Override
    protected RouteBuilder[] createRouteBuilders() {
        var callee = new RouteBuilder() {
            @Override
            public void configure() {
                // Declines, so the caller's clause is the one that fires. A step that both marks
                // and throws never reaches the between-steps gate, so the mark leaves on the
                // exception instead of halting the callee — the only way a mark reaches a clause.
                errorHandler(noErrorHandler());

                from("direct:mark-and-throw-boom")
                        .process(ex -> {
                            ex.setRollbackOnly(true);
                            throw new Boom();
                        });
                from("direct:mark-and-throw-other")
                        .process(ex -> {
                            ex.setRollbackOnly(true);
                            throw new OtherBoom();
                        });
                from("direct:mark-and-throw-third")
                        .process(ex -> {
                            ex.setRollbackOnly(true);
                            throw new ThirdBoom();
                        });
            }
        };

        var caller = new RouteBuilder() {
            @Override
            public void configure() {
                onException(Boom.class).continued(true);
                onException(OtherBoom.class).handled(true).setBody(constant("MAPPED"));
                onException(ThirdBoom.class)
                        .handled(true)
                        .setBody(constant("MAPPED"))
                        .markRollbackOnly();

                from("direct:continued")
                        .to("direct:mark-and-throw-boom")
                        .process(ex -> trace.add("resumed mark=" + ex.isRollbackOnly()));

                from("direct:handled")
                        .to("direct:mark-and-throw-other");

                from("direct:handled-then-remarked")
                        .to("direct:mark-and-throw-third");
            }
        };

        return new RouteBuilder[] { callee, caller };
    }

    @Test
    void continuedRevokesAMarkSetInARouteTheCallerDoesNotOwn() {
        var out = template.request("direct:continued", ex -> ex.getIn().setBody("in"));

        assertThat(trace)
                .describedAs("routing carried on, and the mark the callee set is gone")
                .containsExactly("resumed mark=false");
        assertThat(out.isRollbackOnly())
                .describedAs("so an enclosing boundary would commit")
                .isFalse();
    }

    @Test
    void handledRevokesItIdentically_theErasureIsNotContinuedsDoing() {
        var out = template.request("direct:handled", ex -> ex.getIn().setBody("in"));

        assertThat(out.isRollbackOnly())
                .describedAs("handled(true) erases the mark exactly as continued(true) does, "
                        + "because :1462 runs for both before either branch — so 'continued(true) "
                        + "erases a rollback mark' names the wrong construct")
                .isFalse();
        assertThat(out.getMessage().getBody(String.class)).isEqualTo("MAPPED");
    }

    @Test
    void onlyAMarkSetInsideTheClauseBodySurvives() {
        var out = template.request("direct:handled-then-remarked", ex -> ex.getIn().setBody("in"));

        assertThat(out.isRollbackOnly())
                .describedAs("which is why handled(true) + markRollbackOnly() works: the clear at "
                        + ":1462 happens before the clause body runs, so the mark the body sets is "
                        + "a new one and is never seen by it")
                .isTrue();
        assertThat(out.getMessage().getBody(String.class)).isEqualTo("MAPPED");
    }
}
