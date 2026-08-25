package sandbox;

import org.apache.camel.CamelContext;
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
 * What the HTTP edge does when routes drain slower than requests arrive.
 * <p>
 * Every other boundary in the guide tells the producer something when it is full — an exception, a
 * block, a discard, a fallback. This one is worth measuring precisely because it is the boundary
 * users actually arrive through, and because nothing in the DSL suggests a queue exists.
 */
public class HttpBackpressureProbe extends ProbeSupport {

    private static final int REQUESTS = 40;

    private static int port;
    private final AtomicInteger inFlight = new AtomicInteger();
    private final AtomicInteger peak = new AtomicInteger();
    private final CountDownLatch release = new CountDownLatch(1);
    private final CountDownLatch settled = new CountDownLatch(1);

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
    protected void doPostTearDown() {
        release.countDown();
    }

    @Override
    protected RouteBuilder createRouteBuilder() {
        return new RouteBuilder() {
            @Override
            public void configure() {
                from("platform-http:/slow")
                        .process(ex -> {
                            peak.accumulateAndGet(inFlight.incrementAndGet(), Math::max);
                            settled.countDown();
                            release.await(20, TimeUnit.SECONDS);
                            inFlight.decrementAndGet();
                        })
                        .setBody(constant("done"));
            }
        };
    }

    @Test
    void excessRequestsQueueInvisibly_ratherThanBeingRefused() throws Exception {
        var client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        var responses = new ArrayList<CompletableFuture<HttpResponse<String>>>();

        for (int i = 0; i < REQUESTS; i++) {
            responses.add(client.sendAsync(
                    HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/slow"))
                            .timeout(Duration.ofSeconds(30)).GET().build(),
                    HttpResponse.BodyHandlers.ofString()));
        }

        assertThat(settled.await(10, TimeUnit.SECONDS)).isTrue();
        for (int i = 0; i < 100 && peak.get() < 20; i++) {
            Thread.sleep(50);        // wait for the pool to fill rather than assuming it has
        }
        Thread.sleep(300);           // then a grace, so "exactly 20" rules out overshoot too

        assertThat(peak.get())
                .describedAs("forty requests in flight and twenty routes running: execution is "
                        + "capped by the Vert.x worker pool, whose default size is 20. That pool "
                        + "is the concurrency bound of the whole edge, and nothing in the route "
                        + "declares it.")
                .isEqualTo(20);

        assertThat(responses.stream().filter(CompletableFuture::isDone).count())
                .describedAs("and none of the excess requests was refused, redirected or answered "
                        + "with an error — they are simply waiting, in a queue nothing in the route "
                        + "mentions and nothing bounds")
                .isZero();

        release.countDown();

        var all = CompletableFuture.allOf(responses.toArray(new CompletableFuture[0]));
        all.get(30, TimeUnit.SECONDS);
        assertThat(responses.stream().map(CompletableFuture::join).map(HttpResponse::statusCode))
                .describedAs("every one of them eventually succeeded — the backlog was latency, "
                        + "invisible until the client's own timeout decides it is not")
                .allMatch(code -> code == 200);
    }
}
