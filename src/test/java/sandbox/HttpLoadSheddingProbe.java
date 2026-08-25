package sandbox;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.platform.http.vertx.VertxPlatformHttpServer;
import org.apache.camel.component.platform.http.vertx.VertxPlatformHttpServerConfiguration;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Whether an HTTP edge can be made to refuse work instead of queueing it.
 * <p>
 * On its own it cannot: excess requests wait in the Vert.x worker pool with nothing bounding them.
 * A bulkhead is the cheapest way to put a ceiling in front of that — a semaphore, no extra pool,
 * no thread hop — and because a fallback clears the exception, the refusal can carry a real body
 * and headers rather than the empty {@code text/plain} a failed exchange would produce.
 */
public class HttpLoadSheddingProbe extends ProbeSupport {

    private static final int PERMITS = 2;
    private static final int REQUESTS = 10;

    private static int port;
    private final AtomicInteger admitted = new AtomicInteger();
    private final CountDownLatch full = new CountDownLatch(PERMITS);
    private final CountDownLatch release = new CountDownLatch(1);
    private final AtomicInteger expired = new AtomicInteger();

    @Override
    protected CamelContext createCamelContext() throws Exception {
        var context = super.createCamelContext();
        try (var s = new ServerSocket(0)) {
            port = s.getLocalPort();
        }
        var conf = new VertxPlatformHttpServerConfiguration();
        conf.setBindPort(port);
        context.addService(new VertxPlatformHttpServer(conf));
        return context;
    }

    @Override
    protected RouteBuilder createRouteBuilder() {
        return new RouteBuilder() {
            @Override
            public void configure() {
                from("platform-http:/shed")
                        .circuitBreaker()
                            .resilience4jConfiguration()
                                .bulkheadEnabled(true)
                                .bulkheadMaxConcurrentCalls(PERMITS)
                                .bulkheadMaxWaitDuration(0)
                            .end()
                            .process(ex -> {
                                admitted.incrementAndGet();
                                full.countDown();
                                if (!release.await(30, TimeUnit.SECONDS)) {
                                    expired.incrementAndGet();   // would free a permit early
                                }
                            })
                            .setBody(constant("done"))
                        .onFallback()
                            .setHeader(Exchange.HTTP_RESPONSE_CODE, constant(503))
                            .setHeader("Retry-After", constant("1"))
                            .setBody(constant("{\"error\":\"overloaded\"}"))
                        .end();
            }
        };
    }

    @Test
    void aBulkheadTurnsAnInvisibleQueueIntoAnHonest503() throws Exception {
        var client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        var responses = new ArrayList<CompletableFuture<HttpResponse<String>>>();
        for (int i = 0; i < REQUESTS; i++) {
            responses.add(client.sendAsync(
                    HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/shed"))
                            .timeout(Duration.ofSeconds(30)).GET().build(),
                    HttpResponse.BodyHandlers.ofString()));
        }

        assertThat(full.await(10, TimeUnit.SECONDS)).isTrue();
        Thread.sleep(1000);   // a shed response returns at once; an admitted one is parked

        var shed = new ArrayList<HttpResponse<String>>();
        for (var r : responses) {
            if (r.isDone()) {
                shed.add(r.join());
            }
        }
        assertThat(expired.get()).describedAs("no permit was freed by a hold expiring").isZero();

        assertThat(shed)
                .describedAs("everything past the permit count was refused immediately instead of "
                        + "queueing — the edge now has a ceiling it declares")
                .hasSize(REQUESTS - PERMITS);
        assertThat(shed).allSatisfy(r -> {
            assertThat(r.statusCode()).isEqualTo(503);
            assertThat(r.headers().firstValue("Retry-After")).contains("1");
            assertThat(r.body())
                    .describedAs("and the refusal carries a real body, because the fallback cleared "
                            + "the exception before the consumer looked — an unhandled overload "
                            + "would have produced an empty text/plain instead")
                    .isEqualTo("{\"error\":\"overloaded\"}");
        });

        release.countDown();
        CompletableFuture.allOf(responses.toArray(new CompletableFuture[0])).get(30, TimeUnit.SECONDS);
        assertThat(admitted.get())
                .describedAs("and exactly the permitted number were ever let through")
                .isEqualTo(PERMITS);
    }
}
