package sandbox;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.support.ExchangeHelper;
import org.apache.camel.spring.spi.SpringTransactionPolicy;
import org.apache.camel.test.junit5.CamelTestSupport;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.sql.Statement;

/**
 * Base for probes: an H2 database, a real {@code .transacted()} boundary, and a way to read the
 * stamps Camel leaves on an exchange.
 * <p>
 * Each probe class gets its own in-memory database, dropped and recreated per test, so tests do not
 * have to coordinate. Nothing here starts a container.
 */
public abstract class ProbeSupport extends CamelTestSupport {

    protected DataSource dataSource;

    @Override
    protected CamelContext createCamelContext() throws Exception {
        var context = super.createCamelContext();

        var ds = createDataSource();
        this.dataSource = ds;

        var txManager = new DataSourceTransactionManager(ds);
        context.getRegistry().bind("dataSource", DataSource.class, ds);
        context.getRegistry().bind("txManager", txManager);
        // .transacted() with no argument falls back to the policy named PROPAGATION_REQUIRED once
        // more than one is bound, so both can be present and the default stays REQUIRED.
        context.getRegistry().bind("PROPAGATION_REQUIRED", new SpringTransactionPolicy(txManager));
        var requiresNew = new SpringTransactionPolicy(txManager);
        requiresNew.setPropagationBehaviorName("PROPAGATION_REQUIRES_NEW");
        context.getRegistry().bind("PROPAGATION_REQUIRES_NEW", requiresNew);

        // Teardown only: a failing queue consumer can otherwise hold graceful shutdown for the
        // default timeout, which costs far more than the probe itself.
        context.getShutdownStrategy().setTimeout(2);

        return context;
    }

    /** A database per probe class, kept alive for the life of the JVM by DB_CLOSE_DELAY. */
    protected String jdbcUrl() {
        return "jdbc:h2:mem:" + getClass().getSimpleName() + ";DB_CLOSE_DELAY=-1";
    }

    /** Override to supply a pooled or otherwise bounded DataSource. */
    protected DataSource createDataSource() {
        var ds = new JdbcDataSource();
        ds.setURL(jdbcUrl());
        ds.setUser("sa");
        return ds;
    }

    @BeforeEach
    void resetProbeTable() throws Exception {
        execute("DROP TABLE IF EXISTS probe");
        execute("CREATE TABLE probe (id INT AUTO_INCREMENT PRIMARY KEY, note VARCHAR(64))");
    }

    /** The endpoint that writes one row. Put it inside a transacted route to see what survives. */
    protected static String insertRow(String note) {
        return "sql:INSERT INTO probe (note) VALUES ('" + note + "')?dataSource=#dataSource";
    }

    protected int rows() throws Exception {
        try (Connection c = dataSource.getConnection();
             Statement s = c.createStatement();
             var rs = s.executeQuery("SELECT count(*) FROM probe")) {
            rs.next();
            return rs.getInt(1);
        }
    }

    /**
     * The surviving rows' notes, in insertion order. A count alone cannot tell you which write
     * survived a partial rollback, which is exactly the question most of these probes are asking.
     */
    protected List<String> notes() throws Exception {
        try (Connection c = dataSource.getConnection();
             Statement s = c.createStatement();
             var rs = s.executeQuery("SELECT note FROM probe ORDER BY id")) {
            var out = new ArrayList<String>();
            while (rs.next()) {
                out.add(rs.getString(1));
            }
            return out;
        }
    }

    protected void execute(String sql) throws Exception {
        try (Connection c = dataSource.getConnection(); Statement s = c.createStatement()) {
            s.execute(sql);
        }
    }

    /**
     * The four pieces of state that decide whether routing continues and whether a transaction
     * commits. Printed in the plain vocabulary the design guide uses, not Camel's internal names.
     */
    protected static String stamps(Exchange ex) {
        var xt = ex.getExchangeExtension();
        boolean handled = xt.isErrorHandlerHandledSet() && xt.isErrorHandlerHandled();
        return "exception=" + (ex.getException() != null)
                + " claimed=" + ExchangeHelper.isFailureHandled(ex)
                + " handled=" + handled
                + " rollbackMark=" + ex.isRollbackOnly();
    }

    /** Marker exceptions, so a probe can route different failures to different clauses. */
    public static class Boom extends RuntimeException {
        public Boom() {
            super("boom");
        }
    }

    public static class OtherBoom extends RuntimeException {
        public OtherBoom() {
            super("other boom");
        }
    }
}
