package sandbox;

import org.apache.camel.builder.RouteBuilder;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What a {@code .transacted()} boundary does with a failure that a clause has already mapped.
 * <p>
 * This is the shape that made the whole investigation necessary: a mapped error still returned a
 * response, so the route looked correct, while the rows written before the failure were committed.
 */
public class TransactionProbe extends ProbeSupport {

    @Override
    protected RouteBuilder createRouteBuilder() {
        return new RouteBuilder() {
            @Override
            public void configure() {
                // Maps the failure to a response and stops there. Nothing signals the boundary.
                onException(Boom.class)
                        .handled(true)
                        .setBody(constant("mapped"));

                // The same, plus the mark the boundary actually consults. Last, because setting it
                // halts the clause's own pipeline.
                onException(OtherBoom.class)
                        .handled(true)
                        .setBody(constant("mapped"))
                        .markRollbackOnly();

                from("direct:mapped-only")
                        .transacted()
                        .to(insertRow("written-before-failure"))
                        .process(ex -> {
                            throw new Boom();
                        });

                from("direct:mapped-and-marked")
                        .transacted()
                        .to(insertRow("written-before-failure"))
                        .process(ex -> {
                            throw new OtherBoom();
                        });
            }
        };
    }

    @Test
    void handledAlone_returnsAResponse_butCommitsTheWrites() throws Exception {
        var body = template.requestBody("direct:mapped-only", "in", String.class);

        assertThat(body).describedAs("the caller sees a clean mapped response").isEqualTo("mapped");
        assertThat(rows())
                .describedAs("...and the row written before the failure was committed anyway")
                .isEqualTo(1);
    }

    @Test
    void handledPlusTheMark_returnsTheSameResponse_andRollsBack() throws Exception {
        var body = template.requestBody("direct:mapped-and-marked", "in", String.class);

        assertThat(body).describedAs("the response is unchanged").isEqualTo("mapped");
        assertThat(rows())
                .describedAs("but nothing survives the transaction")
                .isZero();
    }
}
