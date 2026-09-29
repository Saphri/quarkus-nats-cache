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
import io.nats.client.api.KeyValueConfiguration;
import io.nats.client.api.KeyValueStatus;

/**
 * Task 5.5: a bucket that already exists (created externally with different settings) is used unmodified —
 * the extension's add-if-absent provisioning (design D7) must not overwrite its configuration.
 */
@QuarkusTest
class NatsCachePreExistingBucketTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    /** The default bucket derived from the cache name "preexisting". */
    private static final String BUCKET = "PREEXISTING";

    /** History chosen by the external creator; our extension's default is 1, so any overwrite would be visible. */
    private static final int EXTERNAL_HISTORY = 7;

    @Inject
    Client client;

    @Inject
    @CacheName("preexisting")
    Cache cache;

    @BeforeEach
    void recreateExternalBucket() throws Exception {
        var kvm = client.nativeConnection().keyValueManagement();
        try {
            kvm.delete(BUCKET); // idempotent setup: drop leftovers from previous runs
        } catch (JetStreamApiException ignored) {
            // bucket did not exist yet — nothing to delete
        }
        kvm.create(KeyValueConfiguration.builder(BUCKET)
                .description("externally created")
                .maxHistoryPerKey(EXTERNAL_HISTORY)
                .build());
    }

    @Test
    void preExistingBucketIsUsedUnmodified() throws Exception {
        // The cache works against the externally created bucket...
        assertThat(cache.get("k", k -> "v").await().atMost(TIMEOUT)).isEqualTo("v");
        assertThat(cache.get("k", k -> "SHOULD-NOT-RUN").await().atMost(TIMEOUT)).isEqualTo("v");

        // ...and its configuration survived the extension's add-if-absent provisioning.
        KeyValueStatus status = client.nativeConnection().keyValueManagement().getStatus(BUCKET);
        assertThat(status.getMaxHistoryPerKey()).isEqualTo(EXTERNAL_HISTORY);
        assertThat(status.getDescription()).isEqualTo("externally created");
    }
}
