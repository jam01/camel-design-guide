package sandbox;

import org.apache.camel.builder.RouteBuilder;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Where {@code .transacted()} is written in a route does not decide what it covers.
 * <p>
 * The DSL reads as though a step written above the boundary runs outside it, which would make
 * "do the external call first, then open the transaction" a matter of ordering the lines. It is
 * not: {@code RouteDefinitionHelper.initTransacted} moves <em>every</em> non-abstract output of
 * the route into the transacted definition, including the ones written before it, because
 * {@code TransactedDefinition.isAbstract()} takes it out of the route body first.
 */
public class TransactedPositionProbe extends ProbeSupport {

    @Override
    protected RouteBuilder createRouteBuilder() {
        return new RouteBuilder() {
            @Override
            public void configure() {
                // "before" is written above the boundary; "after" below it.
                from("direct:late-transacted")
                        .to(insertRow("before"))
                        .transacted()
                        .to(insertRow("after"))
                        .process(ex -> { throw new Boom(); });

                // Control: the same route with no boundary at all, so both writes autocommit.
                from("direct:untransacted")
                        .to(insertRow("before"))
                        .to(insertRow("after"))
                        .process(ex -> { throw new Boom(); });
            }
        };
    }

    @Test
    void aWriteAboveTheBoundaryIsStillInsideTheTransaction() throws Exception {
        try {
            template.sendBody("direct:late-transacted", "x");
        } catch (Exception expected) {
            // the failure is the point; what survives is the finding
        }

        assertThat(notes())
                .as("a write written above .transacted() is hoisted into it and rolls back with the rest, "
                        + "so ordering an external call before the boundary does not keep it out of the transaction")
                .isEmpty();
    }

    @Test
    void controlWithoutABoundaryBothWritesSurvive() throws Exception {
        try {
            template.sendBody("direct:untransacted", "x");
        } catch (Exception expected) {
            // as above
        }

        assertThat(notes())
                .as("control: without a boundary the same two writes autocommit, "
                        + "so the emptiness above is the transaction and not the failure")
                .containsExactly("before", "after");
    }
}
