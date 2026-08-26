package sandbox;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.support.MessageHelper;

/**
 * Does to an exchange what {@code continued(true)} does, and the one thing it leaves behind — so
 * that a {@code doTry}/{@code doCatch} can recover from a failure without the exchange staying
 * unmappable for the rest of its life.
 * <p>
 * The body mirrors {@code RedeliveryErrorHandler.RedeliveryTask.prepareExchangeForContinue} at
 * {@code camel-4.18.0}, plus {@code setErrorHandlerHandled(null)}. That last flag is not cleared
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
 */
public final class ContinueExchangeProcessor implements Processor {

    @Override
    public void process(Exchange exchange) {
        // as prepareExchangeForContinue
        exchange.setException(null);
        exchange.setRollbackOnly(false);
        MessageHelper.resetStreamCache(exchange.getIn());
        exchange.getIn().removeHeader(Exchange.REDELIVERED);
        exchange.getIn().removeHeader(Exchange.REDELIVERY_COUNTER);
        exchange.getIn().removeHeader(Exchange.REDELIVERY_MAX_COUNTER);
        exchange.getExchangeExtension().setFailureHandled(false);
        // EXCEPTION_CAUGHT is kept on purpose, so the cause is still readable

        // and the parts a clause never has to deal with
        exchange.setRollbackOnlyLast(false);
        exchange.setRouteStop(false);
        exchange.getExchangeExtension().setRedeliveryExhausted(false);
        exchange.getExchangeExtension().setErrorHandlerHandled(null);
    }
}
