package io.quarkiverse.nats.cache.deployment;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.quarkus.cache.Cache;
import io.quarkus.cache.CacheKey;
import io.quarkus.cache.CacheName;
import io.quarkus.cache.CacheResult;
import io.quarkus.cache.CompositeCacheKey;
import io.quarkus.test.junit.QuarkusTest;

/**
 * Task 5.1: round-trip fidelity against the auto-started NATS JetStream dev service (no manual broker config):
 * POJO value type preservation, composite keys, and the AOP {@code @CacheResult} miss→hit path — including
 * {@code @CacheResult} methods with two {@code @CacheKey} parameters (the interceptor composes them into a
 * {@link CompositeCacheKey}, spec scenarios "Composite keys produce distinct entries" and "Composite key with
 * POJO elements needs no registration").
 */
@QuarkusTest
class NatsCacheRoundTripTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Inject
    @CacheName("roundtrip")
    Cache cache;

    @Inject
    Greeter greeter;

    @Inject
    PairService pairService;

    @Inject
    PojoPairService pojoPairService;

    @BeforeEach
    void cleanBucket() {
        // Shared dev-service containers persist between runs; start each test from an empty bucket.
        cache.invalidateAll().await().atMost(TIMEOUT);
    }

    @Test
    void pojoValueRoundTripsWithExactRuntimeType() {
        Order order = new Order("sku-42", 3, new Address("Rue de la Paix", "75002"));
        cache.get("order", k -> order).await().atMost(TIMEOUT);

        Object cached = cache.get("order", k -> new Order("WRONG", 0, null)).await().atMost(TIMEOUT);
        assertThat(cached).isInstanceOf(Order.class); // exact class, not a Map or proxy
        assertThat(cached).isEqualTo(order);
    }

    @Test
    void collectionValueRoundTrips() {
        java.util.List<String> tags = java.util.List.of("a", "b", "c");
        cache.get("tags", k -> tags).await().atMost(TIMEOUT);

        Object cached = cache.get("tags", k -> java.util.List.of("WRONG")).await().atMost(TIMEOUT);
        assertThat(cached).isEqualTo(tags);
    }

    @Test
    void compositeKeysRoundTripAndStayDistinct() {
        CompositeCacheKey first = new CompositeCacheKey("user", 42);
        CompositeCacheKey second = new CompositeCacheKey("user", 43);

        cache.get(first, k -> "v-42").await().atMost(TIMEOUT);
        cache.get(second, k -> "v-43").await().atMost(TIMEOUT);

        assertThat(cache.get(first, k -> "WRONG").await().atMost(TIMEOUT)).isEqualTo("v-42");
        assertThat(cache.get(second, k -> "WRONG").await().atMost(TIMEOUT)).isEqualTo("v-43");
    }

    @Test
    void cacheResultInterceptorMissThenHit() {
        // First call: loader runs and stores. Second call with different args: fresh key, loader runs again.
        assertThat(greeter.greet("alice")).isEqualTo("hello alice");
        assertThat(greeter.loaderInvocations()).isEqualTo(1);

        assertThat(greeter.greet("bob")).isEqualTo("hello bob");
        assertThat(greeter.loaderInvocations()).isEqualTo(2);

        // Third call: "alice" is served from the bucket, loader must not run again.
        assertThat(greeter.greet("alice")).isEqualTo("hello alice");
        assertThat(greeter.loaderInvocations()).isEqualTo(2);
    }

    @Test
    void cacheResultTwoCacheKeyParamsProduceDistinctEntries() {
        // Two @CacheKey params → the interceptor composes a CompositeCacheKey. (a,b) and (a,c) must be
        // independent entries, each served from the bucket on repeat calls without re-invocation.
        assertThat(pairService.pair("user", 42)).isEqualTo("user:42");
        assertThat(pairService.pair("user", 43)).isEqualTo("user:43");
        assertThat(pairService.loaderInvocations()).isEqualTo(2);

        // Repeat calls: both served from the bucket, loader must not run again.
        assertThat(pairService.pair("user", 42)).isEqualTo("user:42");
        assertThat(pairService.pair("user", 43)).isEqualTo("user:43");
        assertThat(pairService.loaderInvocations()).isEqualTo(2);

        // And the two entries did not overwrite each other.
        assertThat(pairService.pair("user", 42)).isEqualTo("user:42");
        assertThat(pairService.loaderInvocations()).isEqualTo(2);
    }

    @Test
    void cacheResultCompositeKeyWithPojoElementNeedsNoRegistration() {
        // POJO key element (KPoint) + String element via the AOP path — reconstructed without any codec
        // registration step, and distinct for distinct elements.
        KPoint p1 = new KPoint(1, 2);
        KPoint p2 = new KPoint(3, 4);

        assertThat(pojoPairService.lookup(p1, "eu")).isEqualTo("KKPoint[x=1, y=2]:eu");
        assertThat(pojoPairService.lookup(p2, "eu")).isEqualTo("KKPoint[x=3, y=4]:eu");
        assertThat(pojoPairService.loaderInvocations()).isEqualTo(2);

        // Equal POJO element (different instance) → same entry, served from the bucket.
        KPoint p1Copy = new KPoint(1, 2);
        assertThat(pojoPairService.lookup(p1Copy, "eu")).isEqualTo("KKPoint[x=1, y=2]:eu");
        assertThat(pojoPairService.loaderInvocations()).isEqualTo(2);
    }

    // ------------------------------------------------------------------ fixtures

    public record Address(String street, String postalCode) {
    }

    public record Order(String sku, int quantity, Address address) {
    }

    @ApplicationScoped
    static class Greeter {

        private final AtomicInteger invocations = new AtomicInteger();

        @CacheResult(cacheName = "roundtrip")
        String greet(String name) {
            invocations.incrementAndGet();
            return "hello " + name;
        }

        int loaderInvocations() {
            return invocations.get();
        }
    }

    @ApplicationScoped
    static class PairService {

        private final AtomicInteger invocations = new AtomicInteger();

        /** Two {@code @CacheKey} parameters → the interceptor composes a {@link CompositeCacheKey}. */
        @CacheResult(cacheName = "roundtrip")
        String pair(@CacheKey String a, @CacheKey int b) {
            invocations.incrementAndGet();
            return a + ":" + b;
        }

        int loaderInvocations() {
            return invocations.get();
        }
    }

    @ApplicationScoped
    static class PojoPairService {

        private final AtomicInteger invocations = new AtomicInteger();

        /** POJO key element + String element — heterogeneous composite, no registration required. */
        @CacheResult(cacheName = "roundtrip")
        String lookup(@CacheKey KPoint point, @CacheKey String region) {
            invocations.incrementAndGet();
            return "K" + point + ":" + region;
        }

        int loaderInvocations() {
            return invocations.get();
        }
    }
}
