package sandbox;

import com.arjuna.ats.jta.TransactionManager;
import jakarta.transaction.Status;
import jakarta.transaction.Synchronization;
import org.apache.camel.jta.JtaTransactionPolicy;

import java.nio.file.Files;
import java.util.function.Consumer;

/**
 * {@code PROPAGATION_REQUIRED} over a real JTA transaction manager: a faithful copy of Quarkus's
 * {@code TransactionalJtaTransactionPolicy.runWithTransaction} — begin if this is the outermost,
 * run, roll back and rethrow on any throwable, otherwise commit. Reproducing the application's policy
 * rather than inventing one is what lets these probes say anything about the application.
 * <p>
 * Shared on purpose. Three probes each kept their own copy, and when one was found to leak a
 * transaction on a setup failure, the fault had already been copied twice — because the part worth
 * reusing (the fidelity) came bundled with scaffolding nobody re-read. A probe can be correct about
 * the thing under test and wrong about its own machinery, and the machinery is what gets copied.
 */
public final class NarayanaRequiredPolicy extends JtaTransactionPolicy {

    private static final Consumer<String> IGNORE = value -> {
    };

    static {
        // Narayana reads this once per JVM, so it has to be set before the first transaction and
        // there is no point setting it again per test.
        try {
            var dir = Files.createTempDirectory("narayana");
            dir.toFile().deleteOnExit();
            System.setProperty("ObjectStoreEnvironmentBean.objectStoreDir", dir.toString());
            System.setProperty("com.arjuna.ats.arjuna.objectstore.objectStoreDir", dir.toString());
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final Consumer<String> onOutcome;
    private final Consumer<String> onErrorHandlerFrame;

    public NarayanaRequiredPolicy() {
        this(IGNORE, IGNORE);
    }

    /** @param onOutcome receives "COMMITTED" or "ROLLED_BACK" once the transaction completes. */
    public NarayanaRequiredPolicy(Consumer<String> onOutcome) {
        this(onOutcome, IGNORE);
    }

    /** @param onErrorHandlerFrame receives each error-handler class on the stack at transaction open. */
    public NarayanaRequiredPolicy(Consumer<String> onOutcome, Consumer<String> onErrorHandlerFrame) {
        this.onOutcome = onOutcome;
        this.onErrorHandlerFrame = onErrorHandlerFrame;
    }

    @Override
    public void run(Runnable runnable) throws Throwable {
        var tm = TransactionManager.transactionManager();
        boolean isNew = tm.getStatus() == Status.STATUS_NO_TRANSACTION
                || tm.getStatus() == Status.STATUS_MARKED_ROLLBACK;

        if (isNew) {
            tm.begin();
        }
        // Everything after begin() belongs inside the try. Anything throwing between the two —
        // constructing the Synchronization below is the likeliest candidate — would leave the
        // transaction associated with the thread, and the NEXT test in the JVM fails with
        // ARJUNA016051 rather than with its own problem. One stale class file is enough.
        try {
            if (isNew) {
                tm.getTransaction().registerSynchronization(new Synchronization() {
                    @Override
                    public void beforeCompletion() {
                    }

                    @Override
                    public void afterCompletion(int status) {
                        onOutcome.accept(status == Status.STATUS_COMMITTED
                                ? "COMMITTED" : "ROLLED_BACK");
                    }
                });
            }
            for (var frame : Thread.currentThread().getStackTrace()) {
                if (frame.getClassName().contains("ErrorHandler")) {
                    onErrorHandlerFrame.accept(frame.getClassName());
                }
            }
            runnable.run();
        } catch (Throwable e) {
            if (isNew) {
                tm.rollback();
            } else {
                tm.setRollbackOnly();
            }
            throw e;
        }
        if (isNew) {
            tm.commit();
        }
    }
}
