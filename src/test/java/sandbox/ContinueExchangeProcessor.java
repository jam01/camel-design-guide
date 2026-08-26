package sandbox;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.support.MessageHelper;

/**
 * Does to an exchange what {@code continued(true)} does, and the one thing it leaves behind — so
 * that a {@code doTry}/{@code doCatch} can recover from a failure without the exchange staying
 * unmappable for the rest of its life.
 * <p>
 * It takes the flag-clearing half of
 * {@code RedeliveryErrorHandler.RedeliveryTask.prepareExchangeForContinue} at
 * {@code camel-4.18.0}, plus {@code setErrorHandlerHandled(null)}, and leaves the rest alone. That last flag is not cleared
 * there because it does not need to be: {@code continued(true)} is a clause, and a clause is only
 * reached on an exchange nothing has claimed, so it never inherits a verdict. A catch block does.
 * <p>
 * Note there are <em>two</em> methods of that name in {@code RedeliveryErrorHandler}, and they do
 * not agree — {@code SimpleTask}'s clears {@code EXCEPTION_CAUGHT} and sets
 * {@code errorHandlerHandled(true)} while never clearing the claim. It cannot apply here:
 * {@code simpleTask} is only chosen when there are no exception policies at all, and a
 * {@code continued(true)} clause is itself an exception policy. {@code RedeliveryTask}'s copy is
 * the one that runs, and the one mirrored below.
 * <p>
 * <b>This reaches into {@code ExchangeExtension}, which is not application-facing API and carries
 * no compatibility promise.</b> It exists because Camel offers no supported way to resume from a
 * failure outside a clause. The intent is to raise this upstream — a lever on
 * {@code doCatch} that performs the same reset — and to delete this class when there is one.
 * <p>
 * It also discards the record that a failure occurred: {@code EXCEPTION_CAUGHT} is deliberately
 * kept, as {@code continued(true)} keeps it, but nothing else remains. Anything downstream that
 * would consult the error state — a completion hook, a compensation, an audit trail — sees a clean
 * exchange afterwards.
 * <p>
 * <b>Place it inside the {@code doCatch}, not after {@code end()}.</b> A catch snapshots
 * {@code routeStop}, {@code rollbackOnly} and {@code rollbackOnlyLast} on entry and restores them
 * in its callback, so an exchange that carried a rollback mark stops routing at the first step
 * after {@code end()} and a repair placed there is never reached. The mark is also invisible from
 * within the body (it reads {@code false}) and a write to it is discarded on the way out — so
 * keeping it is not a choice this class could reverse even if it wanted to. The two flags above are
 * in neither snapshot, which is exactly why they are the two that can be cleared here.
 * {@code routeStop} and {@code redeliveryExhausted} are not set, because the catch has already
 * cleared both before the body runs and clears {@code redeliveryExhausted} again afterwards.
 * All measured in {@code CatchRestoreProbe}.
 */
public final class ContinueExchangeProcessor implements Processor {

    @Override
    public void process(Exchange exchange) {
        // These two are the whole repair. Everything else prepareExchangeForContinue does is either
        // already done by the catch or cannot be done from inside one - see CatchRestoreProbe.
        exchange.getExchangeExtension().setFailureHandled(false);      // the claim
        exchange.getExchangeExtension().setErrorHandlerHandled(null);  // and the verdict

        // Message-level hygiene, mirroring the same method: the failed attempt may have consumed
        // the body, and redelivery headers left by the stage's own error handler would otherwise
        // make isRedelivered() describe a delivery that is over.
        MessageHelper.resetStreamCache(exchange.getIn());
        exchange.getIn().removeHeader(Exchange.REDELIVERED);
        exchange.getIn().removeHeader(Exchange.REDELIVERY_COUNTER);
        exchange.getIn().removeHeader(Exchange.REDELIVERY_MAX_COUNTER);
    }
}
