package sandbox;

import org.apache.camel.builder.RouteBuilder;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Where compensation for non-transactional work can hang, and what the completion hooks actually
 * see once a clause has mapped a failure to a response.
 * <p>
 * The transaction covers the database and nothing else. An object store write or an outbound call
 * survives the rollback and has to be undone by hand, so {@code onCompletion} is the obvious place
 * to hang that undo — and the obvious place to hang the next stage's trigger. These probes
 * establish which of the two hooks fires, and when relative to the commit.
 */
public class CompensationProbe extends ProbeSupport {

    /** Stands in for work a rollback cannot reach: an object written to a bucket. */
    private final List<String> bucket = new CopyOnWriteArrayList<>();
    /** What each hook recorded, and how many committed rows it could see when it ran. */
    private final List<String> hooks = new CopyOnWriteArrayList<>();

    private void record(String hook) {
        try {
            hooks.add(hook + ":committedRows=" + rows());
        } catch (Exception e) {
            hooks.add(hook + ":unreadable");
        }
    }

    @Override
    protected RouteBuilder createRouteBuilder() {
        return new RouteBuilder() {
            @Override
            public void configure() {
                onCompletion().onFailureOnly().process(ex -> record("failure"));
                onCompletion().onCompleteOnly().process(ex -> record("complete"));

                // Maps the failure to a response and signals the boundary — the shape the guide
                // prescribes for a request/reply edge.
                onException(Boom.class)
                        .handled(true)
                        .setBody(constant("mapped"))
                        .markRollbackOnly();

                // Leaves the failure live.
                onException(OtherBoom.class)
                        .setBody(constant("not-mapped"));

                from("direct:mapped")
                        .transacted()
                        .to(insertRow("row"))
                        .process(ex -> bucket.add("object"))
                        .process(ex -> {
                            throw new Boom();
                        });

                from("direct:unmapped")
                        .transacted()
                        .to(insertRow("row"))
                        .process(ex -> bucket.add("object"))
                        .process(ex -> {
                            throw new OtherBoom();
                        });

                from("direct:ok")
                        .transacted()
                        .to(insertRow("row"));
            }
        };
    }

    /** Does not rethrow, so a route that leaves the exception live is still readable. */
    private void run(String uri) {
        template.request(uri, ex -> ex.getIn().setBody("in"));
    }

    @Test
    void aRollbackUndoesTheRow_andLeavesTheObjectOrphaned() throws Exception {
        run("direct:unmapped");

        assertThat(rows()).describedAs("the database work is gone").isZero();
        assertThat(bucket)
                .describedAs("the bucket write was never in the transaction and survives it — the orphan")
                .containsExactly("object");
    }

    @Test
    void mappingAFailureToAResponse_firesTheSUCCESSHook_whileTheTransactionRollsBack() throws Exception {
        run("direct:mapped");

        assertThat(rows()).describedAs("the transaction did roll back").isZero();
        assertThat(hooks)
                .describedAs("handled(true) cleared the exception, so the unit of work saw a success. "
                        + "onCompleteOnly fires on a request that rolled back and returned an error "
                        + "to the caller — anything hung there triggers work that did not happen.")
                .containsExactly("complete:committedRows=0");
    }

    @Test
    void onFailureOnly_firesOnlyWhileTheExceptionIsStillLive() {
        run("direct:unmapped");

        assertThat(hooks)
                .describedAs("no clause cleared the exception, so the unit of work saw a failure")
                .containsExactly("failure:committedRows=0");
    }

    @Test
    void onCompleteOnly_runsAfterTheCommit_soItCanSeeTheRow() {
        run("direct:ok");

        assertThat(hooks)
                .describedAs("the hook reads through a fresh connection, so a visible row means the "
                        + "transaction had already committed when the hook ran")
                .containsExactly("complete:committedRows=1");
    }
}
