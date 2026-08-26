package sandbox;

import org.apache.camel.RollbackExchangeException;
import org.apache.camel.builder.RouteBuilder;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What a catch puts <em>back</em> after its body has run, and what it does not — which is what
 * decides where a repair may be placed and which flags it can touch.
 * <p>
 * {@code CatchProcessor.process} snapshots {@code routeStop}, {@code rollbackOnly} and
 * {@code rollbackOnlyLast} on entry, clears all three so the catch body can run, and restores them
 * in its completion callback. So those three are not merely awkward to change from inside a catch:
 * a write to them there is overwritten on the way out, and the value the body reads is not the
 * value the exchange had.
 * <p>
 * {@code failureHandled} and {@code errorHandlerHandled} are in neither list, which is why they are
 * the two a repair can actually clear.
 */
public class CatchRestoreProbe extends ProbeSupport {

    private final List<String> trace = new CopyOnWriteArrayList<>();

    @Override
    protected RouteBuilder[] createRouteBuilders() {
        var rb = new RouteBuilder() {
            @Override
            public void configure() {
                // No clauses anywhere in this builder: the default error handler claims the
                // callee's failure and leaves the exception live, which is the state a catch
                // in a real recovery meets.
                from("direct:claiming-callee")
                        .process(ex -> {
                            throw new Boom();
                        });

                from("direct:marked")
                        .doTry()
                            .process(ex -> {
                                ex.setRollbackOnly(true);
                                throw new Boom();
                            })
                        .doCatch(Boom.class)
                            .process(ex -> trace.add("in-catch mark=" + ex.isRollbackOnly()))
                        .end()
                        .process(ex -> trace.add("after-end mark=" + ex.isRollbackOnly()));

                from("direct:clear-the-mark-inside")
                        .doTry()
                            .process(ex -> {
                                ex.setRollbackOnly(true);
                                throw new Boom();
                            })
                        .doCatch(Boom.class)
                            .process(ex -> ex.setRollbackOnly(false))
                        .end()
                        .process(ex -> trace.add("after-end mark=" + ex.isRollbackOnly()));

                from("direct:flags")
                        .doTry()
                            .to("direct:claiming-callee")
                        .doCatch(Boom.class)
                            .process(ex -> trace.add("in-catch claimed="
                                    + ex.getExchangeExtension().isFailureHandled()
                                    + " verdictSet=" + ex.getExchangeExtension().isErrorHandlerHandledSet()
                                    + " routeStop=" + ex.isRouteStop()
                                    + " exhausted=" + ex.getExchangeExtension().isRedeliveryExhausted()))
                            .process(ex -> {
                                ex.getExchangeExtension().setFailureHandled(false);
                                ex.getExchangeExtension().setErrorHandlerHandled(null);
                            })
                        .end()
                        .process(ex -> trace.add("after-end claimed="
                                + ex.getExchangeExtension().isFailureHandled()
                                + " verdictSet=" + ex.getExchangeExtension().isErrorHandlerHandledSet()));

                // TryProcessor.continueRouting consulting only routeStop governs advancing from
                // the try part to the catch parts — NOT the steps inside the try, which are an
                // ordinary Pipeline and gate on the mark like any other.
                from("direct:unrelated-mark")
                        .doTry()
                            .process(ex -> ex.setRollbackOnly(true))
                            .process(ex -> trace.add("try-child-2-ran"))
                            .process(ex -> {
                                throw new Boom();
                            })
                        .doCatch(Boom.class)
                            .process(ex -> trace.add("in-catch mark=" + ex.isRollbackOnly()))
                        .end();

                // The control: the ordinary pipeline does consult the mark, so the same two steps
                // outside a doTry never reach the second one.
                from("direct:mark-outside-a-try")
                        .process(ex -> ex.setRollbackOnly(true))
                        .process(ex -> trace.add("step-2-ran"));

                // A step that both marks and throws never reaches the between-steps gate, so the
                // mark rides out on the exception instead of halting anything.
                from("direct:marks-and-throws")
                        .process(ex -> {
                            ex.setRollbackOnly(true);
                            throw new Boom();
                        });

                from("direct:catches-a-callees-mark")
                        .doTry()
                            .to("direct:marks-and-throws")
                        .doCatch(Boom.class)
                            .process(ex -> trace.add("in-catch mark=" + ex.isRollbackOnly()))
                        .end()
                        .process(ex -> trace.add("after-end ran"));

                // A mark set inside a doTry with nothing thrown: what is left of the block?
                from("direct:mark-inside-try")
                        .doTry()
                            .process(ex -> ex.setRollbackOnly(true))
                            .process(ex -> trace.add("try-step-2"))
                        .doCatch(Boom.class)
                            .process(ex -> trace.add("catch"))
                        .doFinally()
                            .process(ex -> trace.add("finally"))
                        .end()
                        .process(ex -> trace.add("after-end"));

                // Does route state SET BY THE CATCH BODY survive the restore on the way out?
                // The restore block runs only when the snapshot had something in it, and then
                // overwrites all three from the snapshot.
                from("direct:body-marks-clean-snapshot")
                        .doTry()
                            .to("direct:claiming-callee")
                        .doCatch(Boom.class)
                            .markRollbackOnly()
                        .end();

                from("direct:body-marks-dirty-snapshot")
                        .doTry()
                            .process(ex -> {
                                ex.setRollbackOnlyLast(true);
                                throw new Boom();
                            })
                        .doCatch(Boom.class)
                            .process(ex -> ex.setRouteStop(true))
                            .markRollbackOnly()
                        .end();

                from("direct:rollback-exception")
                        .doTry()
                            .process(ex -> {
                                ex.setRollbackOnly(true);
                                throw new RollbackExchangeException(ex);
                            })
                        .doCatch(RollbackExchangeException.class)
                            .process(ex -> trace.add("in-catch exception=" + (ex.getException() != null)))
                        .end();
            }
        };

        return new RouteBuilder[] { rb };
    }

    @Test
    void theRollbackMarkIsInvisibleInsideTheCatchAndRestoredAfterIt() {
        template.request("direct:marked", ex -> ex.getIn().setBody("in"));

        assertThat(trace)
                .describedAs("the body reads false because the catch cleared it on entry; the step "
                        + "after end() never runs at all, because the mark was put back and "
                        + "PipelineHelper.continueProcessing stops on it")
                .containsExactly("in-catch mark=false");
    }

    @Test
    void clearingTheRollbackMarkInsideTheCatchIsOverwrittenOnTheWayOut() {
        var out = template.request("direct:clear-the-mark-inside", ex -> ex.getIn().setBody("in"));

        assertThat(trace)
                .describedAs("the write is discarded: CatchProcessor restores the snapshot it took "
                        + "on entry, so routing still stops after end()")
                .isEmpty();
        assertThat(out.isRollbackOnly())
                .describedAs("and the mark survives the whole exchange")
                .isTrue();
    }

    @Test
    void routeStopAndRedeliveryExhaustedAreAlreadyClearedBeforeTheBodyRuns() {
        template.request("direct:flags", ex -> ex.getIn().setBody("in"));

        assertThat(trace.get(0))
                .describedAs("a repair inside a catch cannot usefully set either: the catch cleared "
                        + "routeStop on entry and clears redeliveryExhausted both on entry and "
                        + "again in its callback")
                .isEqualTo("in-catch claimed=true verdictSet=true routeStop=false exhausted=false");
    }

    @Test
    void theTwoFlagsThatGateMappingAreNotRestored() {
        template.request("direct:flags", ex -> ex.getIn().setBody("in"));

        assertThat(trace.get(1))
                .describedAs("failureHandled and errorHandlerHandled are in neither snapshot, so "
                        + "clearing them inside the catch is the one repair that survives it")
                .isEqualTo("after-end claimed=false verdictSet=false");
    }

    @Test
    void aMarkInsideATryHaltsTheTryBodyLikeAnywhereElse() {
        var out = template.request("direct:unrelated-mark", ex -> ex.getIn().setBody("in"));

        assertThat(trace)
                .describedAs("TryProcessor.next() returns three PARTS — the try body, the catch "
                        + "clauses, the finally — and its routeStop-only gate governs advancing "
                        + "between those. The try body itself is a Pipeline, so a mark halts it "
                        + "before the next step: no later step throws, and the catch is entered "
                        + "with no exception and exits at CatchProcessor:124")
                .isEmpty();
        assertThat(out.isRollbackOnly()).isTrue();
    }

    @Test
    void outsideATryTheSameMarkHaltsBeforeTheNextStep() {
        template.request("direct:mark-outside-a-try", ex -> ex.getIn().setBody("in"));

        assertThat(trace)
                .describedAs("PipelineHelper.continueProcessing gates on the mark, which is why a "
                        + "clause can only ever meet a mark that arrived with its own exception")
                .isEmpty();
    }

    @Test
    void aMarkSetByTheThrowingStepOfACalleeReachesTheCallersCatch() {
        var out = template.request("direct:catches-a-callees-mark", ex -> ex.getIn().setBody("in"));

        assertThat(trace)
                .describedAs("the gate is a between-steps check and a throwing step never reaches "
                        + "it, so the mark leaves the callee on the exception rather than halting "
                        + "it — and the step after end() does not run, because it was restored")
                .containsExactly("in-catch mark=false");
        assertThat(out.isRollbackOnly()).isTrue();
    }

    @Test
    void aMarkInsideATryLeavesOnlyTheFinallyRunning() {
        var out = template.request("direct:mark-inside-try", ex -> ex.getIn().setBody("in"));

        assertThat(trace)
                .describedAs("the try body halts at the step after the mark, so nothing throws; the "
                        + "catch is entered but exits at CatchProcessor:124 with no exception; the "
                        + "finally still runs; and the outer pipeline stops after end()")
                .containsExactly("finally");
        assertThat(out.isRollbackOnly())
                .describedAs("and the abort stands")
                .isTrue();
    }

    @Test
    void routeStateSetByTheCatchBodySurvivesWhenTheSnapshotWasEmpty() {
        var out = template.request("direct:body-marks-clean-snapshot", ex -> ex.getIn().setBody("in"));

        assertThat(out.isRollbackOnly())
                .describedAs("the restore block is guarded on the snapshot having had something in "
                        + "it, so with a clean snapshot nothing is written back and the body's own "
                        + "mark stands — the ordinary case, since a failure rarely carries marks")
                .isTrue();
    }

    @Test
    void routeStateSetByTheCatchBodyIsOverwrittenWhenTheSnapshotWasNot() {
        var out = template.request("direct:body-marks-dirty-snapshot", ex -> ex.getIn().setBody("in"));

        assertThat(out.isRollbackOnlyLast())
                .describedAs("the snapshot carried rollbackOnlyLast, so the restore block runs")
                .isTrue();
        assertThat(out.isRollbackOnly())
                .describedAs("and it writes all THREE fields back from the snapshot, so the body's "
                        + "markRollbackOnly() is silently discarded")
                .isFalse();
        assertThat(out.isRouteStop())
                .describedAs("as is its setRouteStop(true) — which is why a rethrow is the "
                        + "dependable way to make a catch terminal")
                .isFalse();
    }

    @Test
    void aCaughtRollbackExceptionIsPutBackOnTheExchange() {
        var out = template.request("direct:rollback-exception", ex -> ex.getIn().setBody("in"));

        assertThat(trace)
                .describedAs("cleared for the duration of the body, like any other catch")
                .containsExactly("in-catch exception=false");
        assertThat(out.getException())
                .describedAs("the sharpest form of the restore: with a mark set, a caught "
                        + "RollbackExchangeException is re-thrown onto the exchange after the body, "
                        + "so this catch does not recover from anything")
                .isInstanceOf(RollbackExchangeException.class);
    }
}
