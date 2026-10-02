package io.quarkiverse.nats.cache.runtime;

import java.time.Duration;
import java.util.Optional;

/**
 * Immutable per-cache information captured at build time (design D7).
 *
 * @param name the cache name as configured in Quarkus (e.g. from {@code quarkus.cache.<name>.type=nats})
 * @param bucket the NATS KV bucket backing this cache: the explicit {@code quarkus.nats-cache.caches.<name>.bucket}
 *        value, or the sanitized uppercase cache name when none is set
 * @param ttl optional bucket-level TTL (hard ceiling for entries), applied at bucket creation
 * @param history maximum number of revisions retained per key (1..64)
 */
public record NatsCacheInfo(String name, String bucket, Optional<Duration> ttl, int history) {

    /** NATS KV bucket name charset (subject-safe; verified against the JNats server-side rules). */
    static final String BUCKET_NAME_PATTERN = "[A-Z0-9_-]+";

    /**
     * Derives the cache info from raw configuration.
     *
     * @param name the cache name; must not be null or empty
     * @param explicitBucket the configured bucket name, if any (validated to be NATS-legal)
     * @param ttl optional bucket TTL
     * @param history maximum revisions per key
     */
    public static NatsCacheInfo of(String name, Optional<String> explicitBucket, Optional<Duration> ttl, int history) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Cache name must not be null or empty");
        }
        String bucket = explicitBucket.orElse(defaultBucket(name));
        if (!bucket.matches(BUCKET_NAME_PATTERN)) {
            throw new IllegalArgumentException(
                    "quarkus.nats-cache.caches." + name + ".bucket value '" + bucket
                            + "' is not a valid NATS KV bucket name; it must match " + BUCKET_NAME_PATTERN);
        }
        if (history < 1 || history > 64) {
            throw new IllegalArgumentException(
                    "quarkus.nats-cache.caches." + name + ".history must be between 1 and 64 but was " + history);
        }
        return new NatsCacheInfo(name, bucket, ttl, history);
    }

    /**
     * Default bucket derivation (design D7): uppercase the cache name and replace every character that is not
     * {@code A-Z}, {@code 0-9}, {@code -} or {@code _} with {@code _}. The transform is deliberately simple and
     * predictable; it is <em>not</em> injective (e.g. {@code my.cache} and {@code my_cache} both map to
     * {@code MY_CACHE}), which is why the deployment module fails the build on derived collisions.
     */
    public static String defaultBucket(String cacheName) {
        StringBuilder sb = new StringBuilder(cacheName.length());
        for (int i = 0; i < cacheName.length(); i++) {
            char c = cacheName.charAt(i);
            if ((c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '-') {
                sb.append(c);
            } else if (c >= 'a' && c <= 'z') {
                sb.append(Character.toUpperCase(c));
            } else {
                sb.append('_');
            }
        }
        return sb.toString();
    }

    /**
     * The quarkiverse {@code KeyValueConfiguration} used to provision this cache's bucket (add-if-absent, design D7).
     */
    public io.quarkiverse.reactive.messaging.nats.jetstream.client.store.configuration.KeyValueConfiguration keyValueConfiguration() {
        io.nats.client.api.KeyValueConfiguration.Builder builder = io.nats.client.api.KeyValueConfiguration.builder(bucket);
        ttl.ifPresent(builder::ttl);
        builder.maxHistoryPerKey(history);
        return io.quarkiverse.reactive.messaging.nats.jetstream.client.store.configuration.KeyValueConfiguration.of(builder.build());
    }
}
