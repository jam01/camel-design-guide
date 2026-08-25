package sandbox;

import org.apache.camel.builder.RouteBuilder;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What happens inside an {@code onException} clause: which clause wins when two could fire, whether
 * the route's own state is still readable from it, and what it costs when the clause body itself
 * throws.
 * <p>
 * This matters because compensation for non-transactional work has nowhere else to live. The
 * completion hooks do not fire on a mapped failure, so the undo has to run inside the clause — on
 * an exchange that is already failing.
 */
public class ClauseProbe extends ProbeSupport {

    /** Stands in for the object store: what is in the bucket, and what the clause did about it. */
    private final List<String> bucket = new CopyOnWriteArrayList<>();
    private final List<String> clauseLog = new CopyOnWriteArrayList<>();

    public static class CompensationFailed extends RuntimeException {
        public CompensationFailed() {
            super("the delete failed too");
        }
    }

    /** A third marker, so the clause whose body throws is reachable on its own. */
    public static class UploadFailed extends RuntimeException {
        public UploadFailed() {
            super("upload failed");
        }
    }

    @Override
    protected RouteBuilder createRouteBuilder() {
        return new RouteBuilder() {
            @Override
            public void configure() {
                onException(Boom.class)
                        .handled(true)
                        .setBody(constant("BUILDER"));

                // Compensation keyed off a property the route sets when it does the upload, so a
                // failure before the upload has nothing to undo.
                onException(OtherBoom.class)
                        .handled(true)
                        .process(ex -> {
                            var key = ex.getProperty("uploadedKey", String.class);
                            if (key == null) {
                                clauseLog.add("nothing-to-undo");
                            } else {
                                bucket.remove(key);
                                clauseLog.add("deleted:" + key);
                            }
                        })
                        .setBody(constant("mapped"))
                        .markRollbackOnly();

                // The undo itself fails, on an exchange that is already failing.
                onException(UploadFailed.class)
                        .handled(true)
                        .process(ex -> {
                            clauseLog.add("compensating");
                            throw new CompensationFailed();
                        })
                        .setBody(constant("mapped"))
                        .markRollbackOnly();

                from("direct:compensation-throws")
                        .transacted()
                        .to(insertRow("row"))
                        .process(ex -> {
                            throw new UploadFailed();
                        });

                from("direct:route-scoped")
                        .onException(Boom.class).handled(true).setBody(constant("ROUTE")).end()
                        .process(ex -> {
                            throw new Boom();
                        });

                from("direct:builder-scoped")
                        .process(ex -> {
                            throw new Boom();
                        });

                from("direct:uploaded-then-failed")
                        .transacted()
                        .to(insertRow("row"))
                        .process(ex -> {
                            bucket.add("k-1");
                            ex.setProperty("uploadedKey", "k-1");
                        })
                        .process(ex -> {
                            throw new OtherBoom();
                        });

                from("direct:failed-before-upload")
                        .transacted()
                        .to(insertRow("row"))
                        .process(ex -> {
                            throw new OtherBoom();
                        });
            }
        };
    }

    @Test
    void aRouteScopedClauseBeatsABuilderScopedOne_forTheSameExceptionType() {
        assertThat(template.requestBody("direct:route-scoped", "in", String.class))
                .describedAs("the nearer clause owns it, so compensation can live next to the "
                        + "route whose work it knows how to undo")
                .isEqualTo("ROUTE");

        assertThat(template.requestBody("direct:builder-scoped", "in", String.class))
                .describedAs("and a route without its own clause still falls back to the builder's")
                .isEqualTo("BUILDER");
    }

    @Test
    void theClauseCanReadWhatTheRouteDid_soItCompensatesOnlyWhatHappened() throws Exception {
        template.request("direct:uploaded-then-failed", ex -> ex.getIn().setBody("in"));

        assertThat(clauseLog)
                .describedAs("exchange properties survive the failure, so the key written before "
                        + "the throw is still readable from the clause")
                .containsExactly("deleted:k-1");
        assertThat(bucket).describedAs("the object was undone by hand").isEmpty();
        assertThat(rows()).describedAs("and the row by the transaction").isZero();
    }

    @Test
    void whenTheCompensationThrows_theRollbackSurvives_butTheMappedResponseDoesNot() throws Exception {
        var result = template.request("direct:compensation-throws", ex -> ex.getIn().setBody("in"));

        assertThat(clauseLog).describedAs("the clause did start").containsExactly("compensating");
        assertThat(rows())
                .describedAs("markRollbackOnly() never ran — the clause pipeline halted at the "
                        + "throw. The transaction rolled back anyway, because the compensation's "
                        + "own exception satisfies gate 2 by itself.")
                .isZero();
        assertThat(result.getException())
                .describedAs("the exchange carries the compensation failure, not the original — "
                        + "the first failure has been overwritten and is no longer the one "
                        + "reported. (Spring's manager wraps it in a RuntimeCamelException to "
                        + "force the rollback; camel-jta rethrows unwrapped, so assert the cause.)")
                .hasRootCauseInstanceOf(CompensationFailed.class);
        assertThat(result.getMessage().getBody(String.class))
                .describedAs("setBody never ran either, so the mapped response is gone — a failing "
                        + "compensation costs the caller its error body, not its rollback")
                .isNotEqualTo("mapped");
    }

    @Test
    void aFailureBeforeTheUpload_leavesTheClauseNothingToUndo() {
        template.request("direct:failed-before-upload", ex -> ex.getIn().setBody("in"));

        assertThat(clauseLog)
                .describedAs("absence of the property is how the clause tells the two failure "
                        + "points apart — it must not delete an object that was never written")
                .containsExactly("nothing-to-undo");
    }
}
