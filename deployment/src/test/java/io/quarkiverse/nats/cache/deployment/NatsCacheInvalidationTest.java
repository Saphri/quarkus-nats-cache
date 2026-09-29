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
 * Task 5.2: invalidation semantics — single-key purge, {@code invalidateAll}, and partial-match
 * {@code invalidateIf} (matching keys removed, non-matching ones intact).
 */
@QuarkusTest
class NatsCacheInvalidationTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Inject
    @CacheName("invalidation")
    Cache cache;

    @BeforeEach
    void cleanBucket() {
        cache.invalidateAll().await().atMost(TIMEOUT);
    }

    @Test
    void invalidateSingleKeyLeavesOthersIntact() {
        cache.get("a", k -> "va").await().atMost(TIMEOUT);
        cache.get("b", k -> "vb").await().atMost(TIMEOUT);

        cache.invalidate("a").await().atMost(TIMEOUT);

        AtomicInteger loader = new AtomicInteger();
        assertThat(cache.get("a", k -> {
            loader.incrementAndGet();
            return "va-reloaded";
        }).await().atMost(TIMEOUT)).isEqualTo("va-reloaded");
        assertThat(loader).hasValue(1); // 'a' was really removed

        assertThat(cache.get("b", k -> "WRONG").await().atMost(TIMEOUT)).isEqualTo("vb"); // 'b' untouched
    }

    @Test
    void invalidateAllRemovesEveryEntry() {
        cache.get("a", k -> "1").await().atMost(TIMEOUT);
        cache.get("b", k -> "2").await().atMost(TIMEOUT);
        cache.get("c", k -> "3").await().atMost(TIMEOUT);

        cache.invalidateAll().await().atMost(TIMEOUT);

        AtomicInteger loader = new AtomicInteger();
        for (String key : java.util.List.of("a", "b", "c")) {
            Object value = cache.get(key, k -> {
                loader.incrementAndGet();
                return "reloaded-" + key;
            }).await().atMost(TIMEOUT);
            assertThat(value).isEqualTo("reloaded-" + key);
        }
        assertThat(loader).hasValue(3); // every entry was really removed
    }

    @Test
    void invalidateIfRemovesOnlyMatchingKeys() {
        cache.get("a1", k -> "v-a1").await().atMost(TIMEOUT);
        cache.get("a2", k -> "v-a2").await().atMost(TIMEOUT);
        cache.get("b1", k -> "v-b1").await().atMost(TIMEOUT);

        cache.invalidateIf(key -> key instanceof String s && s.startsWith("a")).await().atMost(TIMEOUT);

        AtomicInteger loader = new AtomicInteger();
        assertThat(cache.get("a1", k -> {
            loader.incrementAndGet();
            return "reloaded-a1";
        }).await().atMost(TIMEOUT)).isEqualTo("reloaded-a1");
        assertThat(cache.get("a2", k -> {
            loader.incrementAndGet();
            return "reloaded-a2";
        }).await().atMost(TIMEOUT)).isEqualTo("reloaded-a2");
        assertThat(loader).hasValue(2); // both matching keys were really removed

        // Non-matching key survived the selective purge.
        assertThat(cache.get("b1", k -> "WRONG-B1").await().atMost(TIMEOUT)).isEqualTo("v-b1");
    }
}
