package io.quarkiverse.nats.cache.runtime;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import io.quarkus.runtime.annotations.ConfigPhase;
import io.quarkus.runtime.annotations.ConfigRoot;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

/**
 * Build-time configuration for the NATS KV cache backend (design D7).
 *
 * <p>Per-cache settings live under {@code quarkus.nats-cache.caches.<name>.<setting>}:
 * <ul>
 *   <li>{@code bucket} — explicit NATS KV bucket name; when absent, the sanitized uppercase cache name is used;</li>
 *   <li>{@code ttl} — bucket-level TTL (hard ceiling for entries), applied when the bucket is created;</li>
 *   <li>{@code history} — maximum number of revisions retained per key (1..64, default {@code 1}).</li>
 * </ul>
 *
 * <p>Caches that have no entry here use all defaults. The mapping is consumed both at build time (validation) and
 * at runtime init (by {@link NatsCacheBuildRecorder}), following the Infinispan cache extension precedent of keeping
 * the build-time config class in the runtime module.
 */
// @ConfigRoot (BUILD_AND_RUN_TIME_FIXED) is required in addition to @ConfigMapping: it makes the mapping
// visible to build steps and lets Quarkus inject a live config proxy into the recorder constructor
// (same pattern as InfinispanCachesBuildTimeConfig).
@ConfigRoot(phase = ConfigPhase.BUILD_AND_RUN_TIME_FIXED)
@ConfigMapping(prefix = "quarkus.nats-cache")
public interface NatsCachesBuildTimeConfig {

    /** Per-cache settings, keyed by cache name. Empty when no cache has NATS-specific settings. */
    Map<String, Cache> caches();

    interface Cache {

        /** Explicit bucket name; when absent the sanitized uppercase cache name is used (see {@link NatsCacheInfo#defaultBucket}). */
        Optional<String> bucket();

        /** Bucket-level TTL (hard ceiling for entries); applied at bucket creation if the bucket does not exist yet. */
        Optional<Duration> ttl();

        /** Maximum number of revisions retained per key (1..64). */
        @WithDefault("1")
        int history();
    }
}
