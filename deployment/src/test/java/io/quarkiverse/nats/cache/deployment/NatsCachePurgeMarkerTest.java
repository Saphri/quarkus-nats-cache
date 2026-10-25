package io.quarkiverse.nats.cache.deployment;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.inject.Inject;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.quarkiverse.nats.cache.runtime.NatsCacheInfo;
import io.quarkiverse.nats.cache.runtime.codec.CacheTypeAllowList;
import io.quarkiverse.nats.cache.runtime.codec.KeyCodec;
import io.quarkiverse.nats.cache.runtime.kv.QuarkiverseKvBucket;
import io.quarkiverse.nats.cache.runtime.kv.ReadEntry;
import io.quarkiverse.reactive.messaging.nats.jetstream.client.Client;
import io.quarkus.cache.Cache;
import io.quarkus.cache.CacheName;
import io.quarkus.test.junit.QuarkusTest;

/**
 * Pins what a marker is to a cache: a purge marker is bucket bookkeeping, never an entry, so the read after
 * an invalidation is a clean miss (an unreadable entry would purge and reload a second time and log a
 * warning about an entry that is not there).
 */
@QuarkusTest
class NatsCachePurgeMarkerTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    /** The bucket derived from the cache name "marker". */
    private static final String BUCKET = "MARKER";

    @Inject
    Client client;

    @Inject
    @CacheName("marker")
    Cache cache;

    @BeforeEach
    void cleanBucket() {
        cache.invalidateAll().await().atMost(TIMEOUT);
    }

    /** The key encoded the same way the "marker" cache stores it: the allow-list only gates decoding. */
    private String encodedKey() {
        return new KeyCodec(new CacheTypeAllowList(Set.<String>of(), Set.<String>of())).encode("m");
    }

    @Test
    void aPurgeMarkerIsNotAReadEntry() {
        cache.get("m", k -> "v").await().atMost(TIMEOUT);
        cache.invalidate("m").await().atMost(TIMEOUT); // leaves a one-second purge marker

        Optional<String> noBucket = Optional.empty();
        Optional<Duration> noTtl = Optional.empty();
        var info = NatsCacheInfo.of("marker", noBucket, noTtl, 1);
        assertThat(info.bucket()).isEqualTo(BUCKET);
        var bucket = new QuarkiverseKvBucket(client, info);

        // The seam must answer a miss for a marker, not an unreadable entry.
        assertThat(bucket.get(encodedKey()).await().atMost(TIMEOUT)).isEqualTo(ReadEntry.miss());
    }

    @Test
    void invalidatedKeyReadsAsAMissThatRecreatesTheEntry() {
        cache.get("m", k -> "v").await().atMost(TIMEOUT);
        cache.invalidate("m").await().atMost(TIMEOUT);

        AtomicInteger loads = new AtomicInteger();
        assertThat(cache.get("m", k -> {
            loads.incrementAndGet();
            return "reloaded";
        }).await().atMost(TIMEOUT)).isEqualTo("reloaded");
        assertThat(loads).hasValue(1); // the invalidated key was really gone

        // A create succeeds over the marker, so the reload is cached rather than reloaded again.
        assertThat(cache.get("m", k -> "WRONG").await().atMost(TIMEOUT)).isEqualTo("reloaded");
        assertThat(loads).hasValue(1);
    }
}
