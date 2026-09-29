package io.quarkiverse.nats.cache.deployment;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import jakarta.inject.Inject;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.quarkiverse.reactive.messaging.nats.jetstream.client.Client;
import io.quarkus.cache.Cache;
import io.quarkus.cache.CacheName;
import io.quarkus.test.junit.QuarkusTest;
import io.nats.client.JetStreamApiException;
import io.nats.client.api.KeyValueStatus;

/**
 * Spec scenario "Bucket auto-created with TTL": the application starts with
 * {@code quarkus.nats-cache.caches.ttlcheck.ttl=2h} and no existing bucket → a bucket is created whose entries
 * expire after two hours (the configured bucket-level TTL is applied by the extension's add-if-absent
 * provisioning, design D7).
 */
@QuarkusTest
class NatsCacheTtlTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    /** The default bucket derived from the cache name "ttlcheck". */
    private static final String BUCKET = "TTLCHECK";

    /** Set in application.properties: quarkus.nats-cache.caches.ttlcheck.ttl=2h. */
    private static final Duration CONFIGURED_TTL = Duration.ofHours(2);

    @Inject
    Client client;

    @Inject
    @CacheName("ttlcheck")
    Cache cache;

    @BeforeEach
    void dropBucket() throws Exception {
        // Guarantee the "no existing bucket" precondition: drop leftovers from previous runs.
        try {
            client.nativeConnection().keyValueManagement().delete(BUCKET);
        } catch (JetStreamApiException ignored) {
            // bucket did not exist yet — nothing to delete
        }
    }

    @Test
    void autoCreatedBucketCarriesConfiguredTtl() throws Exception {
        // The first cache operation provisions the bucket lazily (add-if-absent, with the configured TTL).
        assertThat(cache.get("k", k -> "v").await().atMost(TIMEOUT)).isEqualTo("v");

        KeyValueStatus status = client.nativeConnection().keyValueManagement().getStatus(BUCKET);
        assertThat(status.getBucketName()).isEqualTo(BUCKET);
        assertThat(status.getTtl()).isEqualTo(CONFIGURED_TTL); // the extension's TTL was applied at creation
    }
}
