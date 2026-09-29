package io.quarkiverse.nats.cache.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.QuarkusIntegrationTest;

/**
 * Task 6.1: native-image smoke test (put, get-hit, get-miss, invalidate — plus the native-specific risks:
 * Jackson reflection on POJOs, composite keys, cached nulls and the {@code @CacheResult} interceptor).
 *
 * <p>Runs against the executable produced by quarkus-maven-plugin ({@code -Dnative}) via
 * {@code @QuarkusIntegrationTest}; bean injection is not available there, so the cache is driven over HTTP
 * through {@link CacheSmokeResource} (standard Quarkus pattern). The app listens on
 * {@code quarkus.http.test-port} (default 8081), as set by the test launcher.
 *
 * <p>Run with (Maven Wrapper — pins Maven 3.9.14):
 * <pre>
 *   ./mvnw install -DskipTests
 *   ./mvnw -pl integration-tests verify -Dnative
 * </pre>
 */
@QuarkusIntegrationTest
class NatsKvCacheNativeIT {

    private static final String BASE = "http://localhost:" + System.getProperty("quarkus.http.test-port", "8081") + "/cache";
    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(30))
            .build();

    @BeforeEach
    void cleanBucket() throws Exception {
        // The dev-service NATS server (and bucket) persists between runs: start each test from an empty bucket.
        assertThat(send(HttpRequest.newBuilder(URI.create(BASE)).DELETE().build()).statusCode()).isEqualTo(204);
    }

    @Test
    void backendIsTheNatsKvCache() throws Exception {
        // Guards against a silently-ignored quarkus.cache.type=nats (which would fall back to Caffeine).
        HttpResponse<String> res = send(GET("/backend"));
        assertThat(res.statusCode()).isEqualTo(200);
        assertThat(res.body()).isEqualTo("io.quarkiverse.nats.cache.runtime.NatsKvCacheImpl");
    }

    @Test
    void pojoRoundTripsWithExactRuntimeTypeInNativeImage() throws Exception {
        String order = "{\"sku\":\"sku-42\",\"quantity\":3,\"address\":{\"street\":\"Rue de la Paix\",\"postalCode\":\"75002\"}}";
        assertThat(send(PUT("/order", order)).statusCode()).isEqualTo(200);

        // Hit: the cached POJO comes back with all its fields. An unregistered POJO would serialize to {} natively,
        // and a broken typed decode would yield a Map/empty object instead of the exact class's shape.
        HttpResponse<String> hit = send(GET("/order?sentinel=WRONG"));
        assertThat(hit.statusCode()).isEqualTo(200);
        assertThat(hit.body())
                .contains("\"sku\":\"sku-42\"")
                .contains("\"quantity\":3")
                .contains("\"street\":\"Rue de la Paix\"")
                .doesNotContain("WRONG");
    }

    @Test
    void compositeKeysRoundTripAndStayDistinctInNativeImage() throws Exception {
        assertThat(send(PUT("/composite/user/42", "\"v-42\"")).statusCode()).isEqualTo(200);
        assertThat(send(PUT("/composite/user/43", "\"v-43\"")).statusCode()).isEqualTo(200);

        HttpResponse<String> first = send(GET("/composite/user/42?sentinel=WRONG"));
        HttpResponse<String> second = send(GET("/composite/user/43?sentinel=WRONG"));
        assertThat(first.body()).contains("v-42").doesNotContain("WRONG");
        assertThat(second.body()).contains("v-43").doesNotContain("WRONG");
    }

    @Test
    void cachedNullSuppressesLoaderUntilInvalidatedInNativeImage() throws Exception {
        // Store a cached-null (loader returns null on the miss).
        assertThat(send(POST("/null")).statusCode()).isEqualTo(200);

        // Hit: served as a cached-null → 204, and the sentinel loader must NOT have run.
        HttpResponse<String> hit = send(GET("/null?sentinel=should-not-load"));
        assertThat(hit.statusCode()).isEqualTo(204);
        assertThat(hit.body()).isEmpty();

        // After invalidation the loader runs again: miss → sentinel loaded, stored and returned.
        assertThat(send(HttpRequest.newBuilder(URI.create(BASE + "/nullkey")).DELETE().build()).statusCode()).isEqualTo(204);
        HttpResponse<String> reloaded = send(GET("/null?sentinel=reloaded"));
        assertThat(reloaded.statusCode()).isEqualTo(200);
        assertThat(reloaded.body()).contains("reloaded");
    }

    @Test
    void invalidateAllThenMissReloadsInNativeImage() throws Exception {
        assertThat(send(PUT("/order", "{\"sku\":\"one\",\"quantity\":1,\"address\":null}")).statusCode()).isEqualTo(200);
        HttpResponse<String> hit = send(GET("/order?sentinel=WRONG"));
        assertThat(hit.body()).contains("\"sku\":\"one\"");

        assertThat(send(HttpRequest.newBuilder(URI.create(BASE)).DELETE().build()).statusCode()).isEqualTo(204);
        HttpResponse<String> miss = send(GET("/order?sentinel=WRONG"));
        assertThat(miss.statusCode()).isEqualTo(200);
        assertThat(miss.body()).contains("WRONG"); // loader ran again after invalidation
    }

    @Test
    void cacheResultInterceptorMissThenHitInNativeImage() throws Exception {
        HttpResponse<String> first = send(GET("/greet/alice"));
        assertThat(first.statusCode()).isEqualTo(200);
        assertThat(first.body()).contains("\"value\":\"hello alice\"").contains("\"invocations\":1");

        // Second call: served from the bucket, loader must not run again.
        HttpResponse<String> second = send(GET("/greet/alice"));
        assertThat(second.body()).contains("\"value\":\"hello alice\"").contains("\"invocations\":1");

        // After invalidation the loader runs again.
        assertThat(send(HttpRequest.newBuilder(URI.create(BASE)).DELETE().build()).statusCode()).isEqualTo(204);
        HttpResponse<String> third = send(GET("/greet/alice"));
        assertThat(third.body()).contains("\"value\":\"hello alice\"").contains("\"invocations\":2");
    }

    // ------------------------------------------------------------------ tiny HTTP helpers (JDK client, no extra deps)

    private static HttpRequest GET(String pathAndQuery) {
        return HttpRequest.newBuilder(URI.create(BASE + pathAndQuery)).GET()
                .timeout(Duration.ofSeconds(30)).build();
    }

    private static HttpRequest PUT(String path, String jsonBody) {
        return HttpRequest.newBuilder(URI.create(BASE + path))
                .PUT(HttpRequest.BodyPublishers.ofString(jsonBody))
                .header("Content-Type", "application/json")
                .timeout(Duration.ofSeconds(30)).build();
    }

    private static HttpRequest POST(String path) {
        return HttpRequest.newBuilder(URI.create(BASE + path)).POST(HttpRequest.BodyPublishers.noBody())
                .timeout(Duration.ofSeconds(30)).build();
    }

    private static HttpResponse<String> send(HttpRequest request) throws Exception {
        return HTTP.send(request, HttpResponse.BodyHandlers.ofString());
    }
}
