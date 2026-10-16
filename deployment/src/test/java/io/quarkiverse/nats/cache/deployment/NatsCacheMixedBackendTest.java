package io.quarkiverse.nats.cache.deployment;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import jakarta.inject.Inject;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.quarkiverse.nats.cache.runtime.NatsKvCacheImpl;
import io.quarkiverse.reactive.messaging.nats.jetstream.client.Client;
import io.quarkus.cache.Cache;
import io.quarkus.cache.CacheName;
import io.quarkus.test.junit.QuarkusTest;

/**
 * Task 5.4: two backends in one application — {@code mixednats} (NATS KV, the global default) and
 * {@code mixedcaffeine} (Caffeine, per-name override via {@code quarkus.cache.caches.mixedcaffeine.type}) —
 * plus the per-item expiration of the NATS backend, a server-side per-message TTL (design D6).
 */
@QuarkusTest
class NatsCacheMixedBackendTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    /** The default bucket derived from the cache name "mixednats". */
    private static final String NATS_BUCKET = "MIXEDNATS";

    @Inject
    Client client;

    @Inject
    @CacheName("mixednats")
    Cache natsCache;

    @Inject
    @CacheName("mixedcaffeine")
    Cache caffeineCache;

    /** Whether the stale-bucket check below already ran in this JVM. */
    private static boolean bucketChecked;

    @BeforeEach
    void cleanBuckets() throws Exception {
        if (!bucketChecked) {
            // Dev-service containers persist between runs: a bucket provisioned by an earlier build of the extension
            // may lack per-message TTL support. Drop it before the first cache operation so it is re-provisioned.
            bucketChecked = true;
            try {
                var status = client.nativeConnection().keyValueManagement().getStatus(NATS_BUCKET);
                if (!status.getBackingStreamInfo().getConfiguration().isAllowMessageTtl()) {
                    client.nativeConnection().keyValueManagement().delete(NATS_BUCKET);
                }
            } catch (io.nats.client.JetStreamApiException ignored) {
                // bucket does not exist yet
            }
        }
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

        // Store with a 2 s TTL (dormant upstream overload; plain method on NatsKvCacheImpl).
        assertThat(nats.get("exp", k -> "first", Duration.ofSeconds(2)).await().atMost(TIMEOUT)).isEqualTo("first");

        // Still within the window: served from the bucket, loader must not run.
        assertThat(natsCache.get("exp", k -> "second").await().atMost(TIMEOUT)).isEqualTo("first");

        // After the TTL elapses the server has removed the entry: a miss, so the loader runs again. The wait leaves
        // headroom for the server's expiry sweep, which can lag the TTL by up to about a second.
        Thread.sleep(3500);
        assertThat(natsCache.get("exp", k -> "third").await().atMost(TIMEOUT)).isEqualTo("third");
        // The reload was cached again (created over the expired entry's marker), not just returned.
        assertThat(natsCache.get("exp", k -> "fourth").await().atMost(TIMEOUT)).isEqualTo("third");
    }

    @Test
    void invalidationLeavesNoMarkerBehind() throws Exception {
        natsCache.get("inv", k -> "value").await().atMost(TIMEOUT);
        natsCache.invalidate("inv").await().atMost(TIMEOUT);

        // The purge marker carries a short TTL, so the server removes it as well: the stream ends up empty.
        Thread.sleep(2500);
        assertThat(client.nativeConnection().jetStreamManagement().getStreamInfo("KV_" + NATS_BUCKET)
                .getStreamState().getMsgCount()).isZero();
        // ...and the key can be cached again.
        assertThat(natsCache.get("inv", k -> "again").await().atMost(TIMEOUT)).isEqualTo("again");
        assertThat(natsCache.get("inv", k -> "NOT CALLED").await().atMost(TIMEOUT)).isEqualTo("again");
    }

    @Test
    void unreadableEntryIsReplaced() throws Exception {
        // Writes are create-only, so an unreadable entry must be purged before its reload can be cached.
        client.nativeConnection().keyValue(NATS_BUCKET)
                .put(new io.quarkiverse.nats.cache.runtime.codec.KeyCodec(
                        io.quarkiverse.nats.cache.runtime.codec.CacheTypeAllowList.jdkOnly()).encode("corrupt"), "not an envelope".getBytes());

        assertThat(natsCache.get("corrupt", k -> "reloaded").await().atMost(TIMEOUT)).isEqualTo("reloaded");
        assertThat(natsCache.get("corrupt", k -> "NOT CALLED").await().atMost(TIMEOUT)).isEqualTo("reloaded");
    }

    @Test
    void expiredEntryIsRemovedServerSide() throws Exception {
        NatsKvCacheImpl nats = requireNatsBackend(natsCache);
        io.nats.client.KeyValue bucket = client.nativeConnection().keyValue(NATS_BUCKET);

        nats.get("gone", k -> "value", Duration.ofSeconds(1)).await().atMost(TIMEOUT);
        assertThat(bucket.keys()).hasSize(1);

        // Unlike a logical TTL, the expired entry does not linger until overwritten: the server deletes it.
        Thread.sleep(2500);
        assertThat(bucket.keys()).isEmpty();
    }

    @Test
    void expiredCachedNullNoLongerSuppressesLoading() throws InterruptedException {
        NatsKvCacheImpl nats = requireNatsBackend(natsCache);

        // Store a cached-null with a 2 s TTL.
        assertThat(nats.get("expnull", k -> null, Duration.ofSeconds(2)).await().atMost(TIMEOUT)).isNull();

        // Within the window: the sentinel still suppresses the loader → null is emitted.
        java.util.concurrent.atomic.AtomicInteger runs = new java.util.concurrent.atomic.AtomicInteger();
        Object within = natsCache.get("expnull", k -> {
            runs.incrementAndGet();
            return "loaded";
        }).await().atMost(TIMEOUT);
        assertThat(within).isNull();
        assertThat(runs.get()).isZero();

        // After the TTL elapses: the cached-null no longer suppresses → loader runs and overwrites.
        Thread.sleep(3500);
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
