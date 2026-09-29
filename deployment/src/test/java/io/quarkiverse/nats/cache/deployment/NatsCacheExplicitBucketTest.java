package io.quarkiverse.nats.cache.deployment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;

import jakarta.inject.Inject;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.quarkiverse.reactive.messaging.nats.jetstream.client.Client;
import io.quarkus.cache.Cache;
import io.quarkus.cache.CacheName;
import io.quarkus.test.junit.QuarkusTest;
import io.nats.client.JetStreamApiException;
import io.nats.client.support.NatsKeyValueUtil;

/**
 * End-to-end proof of the per-name configuration mapping ({@code quarkus.nats-cache.caches.<name>.bucket}):
 * cache {@code explicit} is configured with bucket {@code EXPLICIT_BKT}, so it must use that stream and its
 * derived default bucket {@code EXPLICIT} must never be created.
 */
@QuarkusTest
class NatsCacheExplicitBucketTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Inject
    Client client;

    @Inject
    @CacheName("explicit")
    Cache cache;

    @BeforeEach
    void cleanBuckets() throws Exception {
        // Shared dev-service containers persist between runs: clear both candidate buckets first.
        deleteQuietly("EXPLICIT_BKT");
        deleteQuietly("EXPLICIT");
    }

    @Test
    void explicitBucketIsUsedAndDerivedDefaultNeverCreated() throws Exception {
        assertThat(cache.get("k", k -> "v").await().atMost(TIMEOUT)).isEqualTo("v");
        assertThat(cache.get("k", k -> "SHOULD-NOT-RUN").await().atMost(TIMEOUT)).isEqualTo("v");

        // The configured bucket exists...
        var management = client.nativeConnection().jetStreamManagement();
        assertThat(management.getStreamInfo(NatsKeyValueUtil.toStreamName("EXPLICIT_BKT"))).isNotNull();

        // ...while the derived default bucket was never provisioned.
        assertThatThrownBy(() -> management.getStreamInfo(NatsKeyValueUtil.toStreamName("EXPLICIT")))
                .isInstanceOf(JetStreamApiException.class)
                .hasMessageContaining("stream not found");
    }

    private void deleteQuietly(String bucket) {
        try {
            client.nativeConnection().jetStreamManagement().deleteStream(NatsKeyValueUtil.toStreamName(bucket));
        } catch (Exception ignored) {
            // stream did not exist — nothing to clean
        }
    }
}
