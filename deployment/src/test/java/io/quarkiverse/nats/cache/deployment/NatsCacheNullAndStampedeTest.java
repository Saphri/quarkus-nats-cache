package io.quarkiverse.nats.cache.deployment;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.inject.Inject;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.quarkus.cache.Cache;
import io.quarkus.cache.CacheName;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.Uni;

/**
 * Task 5.3: a cached {@code null} result suppresses the loader until invalidated, and N concurrent loads of the
 * same key share exactly one loader execution (design D3 stampede protection).
 */
@QuarkusTest
class NatsCacheNullAndStampedeTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Inject
    @CacheName("nullstampede")
    Cache cache;

    @BeforeEach
    void cleanBucket() {
        cache.invalidateAll().await().atMost(TIMEOUT);
    }

    @Test
    void cachedNullSuppressesLoaderUntilInvalidated() {
        AtomicInteger loader = new AtomicInteger();

        // First read: loader runs and returns null; the null is stored as the sentinel.
        Object first = cache.get("n", k -> {
            loader.incrementAndGet();
            return null;
        }).await().atMost(TIMEOUT);
        assertThat(first).isNull();
        assertThat(loader).hasValue(1);

        // Second read: served from the bucket as a cached-null, loader must NOT run again.
        Object second = cache.get("n", k -> {
            loader.incrementAndGet();
            return "should-not-load";
        }).await().atMost(TIMEOUT);
        assertThat(second).isNull();
        assertThat(loader).hasValue(1);

        // After invalidation the loader runs again.
        cache.invalidate("n").await().atMost(TIMEOUT);
        Object third = cache.get("n", k -> {
            loader.incrementAndGet();
            return "reloaded";
        }).await().atMost(TIMEOUT);
        assertThat(third).isEqualTo("reloaded");
        assertThat(loader).hasValue(2);
    }

    @Test
    void concurrentLoadsShareOneLoaderExecution() throws Exception {
        int concurrency = 10;
        AtomicInteger executions = new AtomicInteger();

        ExecutorService pool = Executors.newFixedThreadPool(concurrency);
        try {
            // Slow loader: every concurrent reader observes the in-flight entry before it settles.
            List<Uni<String>> unis = java.util.stream.IntStream.range(0, concurrency)
                    .mapToObj(i -> cache.get("hot", k -> {
                        executions.incrementAndGet();
                        sleepQuietly(500);
                        return "value";
                    }))
                    .toList();

            List<String> results = new java.util.ArrayList<>(concurrency);
            for (Uni<String> uni : unis) {
                results.add(pool.submit(() -> uni.await().atMost(TIMEOUT)).get(30, TimeUnit.SECONDS));
            }

            assertThat(results).allSatisfy(r -> assertThat(r).isEqualTo("value"));
            // All 10 concurrent loads of the same key must collapse into a single loader execution.
            assertThat(executions).hasValue(1);
        } finally {
            pool.shutdownNow();
        }
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
