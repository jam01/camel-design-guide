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
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A real HTTP consumer, in this JVM, so the edge behaviour is measured rather than read.
 * <p>
 * Two things this settles: which thread a route actually runs on behind
 * {@code platform-http-vertx}, and what reaches the client on a failed exchange.
 */
public class HttpConsumerProbe extends ProbeSupport {

    private static int port;
    private final List<String> threads = new CopyOnWriteArrayList<>();

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
                from("platform-http:/thread")
                        .process(ex -> threads.add(Thread.currentThread().getName()))
                        .setBody(constant("ok"));

                // Fails with a status header set, and nothing clears the exception.
                from("platform-http:/unmapped")
                        .setHeader(Exchange.HTTP_RESPONSE_CODE, constant(409))
                        .setBody(constant("{\"error\":\"conflict\"}"))
                        .process(ex -> {
                            throw new Boom();
                        });

                // The same, but the clause clears the exception first.
                from("platform-http:/mapped")
                        .onException(Boom.class)
                            .handled(true)
                            .setHeader(Exchange.HTTP_RESPONSE_CODE, constant(409))
                            .setBody(constant("{\"error\":\"conflict\"}"))
                        .end()
                        .process(ex -> {
                            throw new Boom();
                        });

                // muteException is on by default; turn it off to see the other branch.
                from("platform-http:/unmuted?muteException=false")
                        .setBody(constant("{\"error\":\"conflict\"}"))
                        .process(ex -> {
                            throw new Boom();
                        });
            }
        };
    }

    private HttpResponse<String> get(String path) throws Exception {
        return HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create("http://localhost:" + port + path)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void aRouteRunsOnAVertxWorkerThread_notTheEventLoop() throws Exception {
        assertThat(get("/thread").statusCode()).isEqualTo(200);

        assertThat(threads).hasSize(1);
        assertThat(threads.get(0))
                .describedAs("the consumer wraps route processing in vertx.executeBlocking, so the "
                        + "route gets a worker thread from the shared Vert.x pool — never the event "
                        + "loop. Shared, though: it is not isolated per route.")
                .contains("worker");
        assertThat(threads.get(0)).doesNotContain("eventloop");
    }

    @Test
    void onAFailedExchangeTheStatusSurvivesAndTheBodyDoesNot() throws Exception {
        var res = get("/unmapped");

        assertThat(res.statusCode())
                .describedAs("the status is an ordinary header and survives the failure")
                .isEqualTo(409);
        assertThat(res.body())
                .describedAs("the body is not read at all while an exception is present")
                .isEmpty();
        assertThat(res.headers().firstValue("Content-Type").orElse(""))
                .describedAs("and the content type is forced, so the JSON we set is gone twice over")
                .contains("text/plain");
    }

    @Test
    void clearingTheExceptionIsWhatLetsTheBodyThrough() throws Exception {
        var res = get("/mapped");

        assertThat(res.statusCode()).isEqualTo(409);
        assertThat(res.body())
                .describedAs("same status, same body set the same way — the only difference is "
                        + "that handled(true) removed the exception before the consumer looked")
                .isEqualTo("{\"error\":\"conflict\"}");
    }

    @Test
    void unmutingDoesNotGiveYouTheBody_itGivesYouTheStackTrace() throws Exception {
        var res = get("/unmuted");

        assertThat(res.body())
                .describedAs("muteException=false is not a way to return your own body — the other "
                        + "branch sends the stack trace, and the message body is unreachable either way")
                .contains("sandbox.ProbeSupport$Boom");
        assertThat(res.body()).doesNotContain("conflict");
    }
}
