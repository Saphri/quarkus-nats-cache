package io.quarkiverse.nats.cache.deployment;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import jakarta.inject.Inject;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.quarkiverse.nats.cache.runtime.NatsKvCacheImpl;
import io.quarkus.cache.Cache;
import io.quarkus.cache.CacheName;
import io.quarkus.test.junit.QuarkusTest;

/**
 * Task 5.4: two backends in one application — {@code mixednats} (NATS KV, the global default) and
 * {@code mixedcaffeine} (Caffeine, per-name override via {@code quarkus.cache.caches.mixedcaffeine.type}) —
 * plus the per-item logical TTL of the NATS backend (design D6).
 */
@QuarkusTest
class NatsCacheMixedBackendTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Inject
    @CacheName("mixednats")
    Cache natsCache;

    @Inject
    @CacheName("mixedcaffeine")
    Cache caffeineCache;

    @BeforeEach
    void cleanBuckets() {
        natsCache.invalidateAll().await().atMost(TIMEOUT);
        caffeineCache.invalidateAll().await().atMost(TIMEOUT);
    }

    @Test
    void bothBackendsAreFunctionalAndIndependent() {
        // The two caches really are different backends (guards against a silently ignored type override:
        // with both on NATS they would land on distinct buckets and the functional assertions below still pass).
        assertThat(natsCache).isInstanceOf(NatsKvCacheImpl.class);
        assertThat(caffeineCache)
                .isNotInstanceOf(NatsKvCacheImpl.class)
                .isInstanceOf(io.quarkus.cache.CaffeineCache.class); // Caffeine backend, not NATS

        // NATS backend: miss → hit.
        assertThat(natsCache.get("k", k -> "nats-value").await().atMost(TIMEOUT)).isEqualTo("nats-value");
        assertThat(natsCache.get("k", k -> "WRONG").await().atMost(TIMEOUT)).isEqualTo("nats-value");

        // Caffeine backend: the same cache name + key resolves to a completely different store.
        assertThat(caffeineCache.get("k", k -> "caffeine-value").await().atMost(TIMEOUT)).isEqualTo("caffeine-value");
        assertThat(caffeineCache.get("k", k -> "WRONG").await().atMost(TIMEOUT)).isEqualTo("caffeine-value");

        // The two stores do not leak into each other.
        assertThat(natsCache.get("k", k -> "LEAKED").await().atMost(TIMEOUT)).isEqualTo("nats-value");
    }

    @Test
    void perItemExpirationServedBeforeAndReloadedAfter() throws InterruptedException {
        NatsKvCacheImpl nats = requireNatsBackend(natsCache);

        // Store with a 2 s logical TTL (dormant upstream overload; plain method on NatsKvCacheImpl).
        assertThat(nats.get("exp", k -> "first", Duration.ofSeconds(2)).await().atMost(TIMEOUT)).isEqualTo("first");

        // Still within the window: served from the bucket, loader must not run.
        assertThat(natsCache.get("exp", k -> "second").await().atMost(TIMEOUT)).isEqualTo("first");

        // After the logical TTL elapses: the entry is treated as a miss and the loader runs again.
        Thread.sleep(2500);
        assertThat(natsCache.get("exp", k -> "third").await().atMost(TIMEOUT)).isEqualTo("third");
    }

    @Test
    void expiredCachedNullNoLongerSuppressesLoading() throws InterruptedException {
        NatsKvCacheImpl nats = requireNatsBackend(natsCache);

        // Store a cached-null with a 2 s logical TTL.
        assertThat(nats.get("expnull", k -> null, Duration.ofSeconds(2)).await().atMost(TIMEOUT)).isNull();

        // Within the window: the sentinel still suppresses the loader → null is emitted.
        java.util.concurrent.atomic.AtomicInteger runs = new java.util.concurrent.atomic.AtomicInteger();
        Object within = natsCache.get("expnull", k -> {
            runs.incrementAndGet();
            return "loaded";
        }).await().atMost(TIMEOUT);
        assertThat(within).isNull();
        assertThat(runs.get()).isZero();

        // After the logical TTL elapses: the cached-null no longer suppresses → loader runs and overwrites.
        Thread.sleep(2500);
        Object after = natsCache.get("expnull", k -> {
            runs.incrementAndGet();
            return "loaded";
        }).await().atMost(TIMEOUT);
        assertThat(after).isEqualTo("loaded");
        assertThat(runs.get()).isEqualTo(1);
    }

    private static NatsKvCacheImpl requireNatsBackend(Cache cache) {
        if (!(cache instanceof NatsKvCacheImpl nats)) {
            throw new AssertionError("Expected the 'mixednats' cache to be backed by NatsKvCacheImpl but was "
                    + cache.getClass().getName());
        }
        return nats;
    }
}
