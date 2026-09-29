package io.quarkiverse.nats.cache.deployment;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.inject.Inject;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.quarkus.cache.Cache;
import io.quarkus.cache.CacheName;
import io.quarkus.test.junit.QuarkusTest;

/**
 * End-to-end smoke test (tasks 4.1/4.3): proves the extension is discovered, the recorder wiring works,
 * and a {@code nats}-typed cache stores and serves values through a real NATS JetStream KV bucket provided
 * by the dev service — with no manual broker configuration.
 */
@QuarkusTest
class NatsCacheSmokeTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Inject
    @CacheName("smoke")
    Cache smokeCache;

    private final AtomicInteger loaderInvocations = new AtomicInteger();

    @BeforeEach
    void cleanBucket() {
        // Shared dev-service containers persist between runs; start each test from an empty bucket.
        smokeCache.invalidateAll().await().atMost(TIMEOUT);
    }

    @Test
    void missThenHit() {
        String loaded = smokeCache.get("k1", k -> {
            loaderInvocations.incrementAndGet();
            return "v1";
        }).await().atMost(TIMEOUT);
        assertThat(loaded).isEqualTo("v1");

        // Second read must be served from the bucket; the loader must not run again.
        String cached = smokeCache.get("k1", k -> "SHOULD-NOT-RUN").await().atMost(TIMEOUT);
        assertThat(cached).isEqualTo("v1");
        assertThat(loaderInvocations.get()).isEqualTo(1);
    }

    @Test
    void invalidationRemovesEntry() {
        smokeCache.get("k2", k -> "original").await().atMost(TIMEOUT);
        smokeCache.invalidate("k2").await().atMost(TIMEOUT);

        loaderInvocations.set(0);
        String reloaded = smokeCache.get("k2", k -> {
            loaderInvocations.incrementAndGet();
            return "reloaded";
        }).await().atMost(TIMEOUT);
        assertThat(reloaded).isEqualTo("reloaded");
        assertThat(loaderInvocations.get()).isEqualTo(1);
    }
}
