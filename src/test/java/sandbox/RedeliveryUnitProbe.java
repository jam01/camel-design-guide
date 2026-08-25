package sandbox;

import org.apache.camel.builder.RouteBuilder;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What a redelivery actually re-runs: the whole route, or the step that failed.
 * <p>
 * This decides whether retry is safe on a route that has already done something. If redelivery
 * replayed the route from the top, every attempt would repeat the earlier steps and any side
 * effect in them.
 */
public class RedeliveryUnitProbe extends ProbeSupport {

    private final List<String> before = new CopyOnWriteArrayList<>();
    private final List<String> failing = new CopyOnWriteArrayList<>();

    @Override
    protected RouteBuilder createRouteBuilder() {
        return new RouteBuilder() {
            @Override
            public void configure() {
                errorHandler(defaultErrorHandler().maximumRedeliveries(2).redeliveryDelay(0));

                from("direct:two-steps")
                        .process(ex -> before.add("ran"))
                        .process(ex -> {
                            failing.add("ran");
                            throw new Boom();
                        });

                from("direct:transacted-two-steps")
                        .transacted()
                        .to(insertRow("row"))
                        .process(ex -> {
                            failing.add("ran");
                            throw new Boom();
                        });
            }
        };
    }

    @Test
    void redeliveryResumesAtTheFailingStep_itDoesNotReplayTheRoute() {
        template.request("direct:two-steps", ex -> ex.getIn().setBody("in"));

        assertThat(failing).describedAs("the failing step ran three times").hasSize(3);
        assertThat(before)
                .describedAs("but the step before it ran once — redelivery is scoped to the "
                        + "processor that threw, not to the route. Work already done is not "
                        + "repeated, and a retry cannot undo it either.")
                .hasSize(1);
    }

    @Test
    void insideATransaction_theEarlierWriteIsNotRepeatedByRetries() throws Exception {
        template.request("direct:transacted-two-steps", ex -> ex.getIn().setBody("in"));

        assertThat(failing).hasSize(3);
        assertThat(rows())
                .describedAs("all three attempts happened inside one transaction, which then "
                        + "rolled back — the insert was neither repeated nor kept")
                .isZero();
    }
}
