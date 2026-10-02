package io.quarkiverse.nats.cache.deployment;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.Test;

import io.quarkiverse.nats.cache.runtime.NatsCachesBuildTimeConfig;
import io.quarkus.cache.runtime.CacheBuildConfig;

/**
 * Build-time bucket collision validation (design D8, spec scenario *Default bucket collision fails the build*
 * and *Explicitly shared bucket allowed*).
 */
class BucketCollisionValidatorTest {

    private static final Set<String> COLLIDING_NAMES = new HashSet<>(Set.of("my.cache", "my_cache"));

    @Test
    void derivedBucketCollisionIsReportedNamingBothCaches() {
        List<String> errors = BucketCollisionValidator.findCollisions(
                COLLIDING_NAMES, config("nats", Map.of()), natsConfig(Map.of()));
        assertThat(errors).hasSize(1);
        assertThat(errors.get(0))
                .contains("MY_CACHE")
                .contains("'my.cache'")
                .contains("'my_cache'")
                .contains("quarkus.nats-cache.caches.<name>.bucket");
    }

    @Test
    void explicitlySharedBucketIsAllowed() {
        assertThat(BucketCollisionValidator.findCollisions(
                Set.of("alpha", "beta"), config("nats", Map.of()),
                natsConfig(Map.of("alpha", bucket("SHARED"), "beta", bucket("SHARED")))))
                        .isEmpty();
    }

    @Test
    void explicitBucketCollidingWithDerivedOneIsReported() {
        // 'x' explicitly chooses Y_CACHE while 'y_cache' derives to the same bucket: still an accidental
        // collision (invalidateAll on one cache would wipe the other's entries), so it must be reported.
        List<String> errors = BucketCollisionValidator.findCollisions(
                Set.of("x", "y_cache"), config("nats", Map.of()),
                natsConfig(Map.of("x", bucket("Y_CACHE"))));
        assertThat(errors).hasSize(1);
        assertThat(errors.get(0))
                .contains("Y_CACHE")
                .contains("'x'")
                .contains("'y_cache'");
    }

    @Test
    void nonNatsCachesAreIgnored() {
        // Same derived-bucket pattern, but the caches are of another backend type.
        assertThat(BucketCollisionValidator.findCollisions(
                COLLIDING_NAMES, config("caffeine", Map.of()), natsConfig(Map.of())))
                        .isEmpty();
    }

    @Test
    void perNameTypeOverrideExcludesCacheFromValidation() {
        // 'my_cache' is overridden to another backend, so only one nats cache remains on MY_CACHE.
        Map<String, String> overrides = new HashMap<>();
        overrides.put("my_cache", "caffeine");
        assertThat(BucketCollisionValidator.findCollisions(
                COLLIDING_NAMES, config("nats", overrides), natsConfig(Map.of())))
                        .isEmpty();
    }

    @Test
    void distinctDerivedBucketsAreFine() {
        assertThat(BucketCollisionValidator.findCollisions(
                Set.of("alpha", "beta"), config("nats", Map.of()), natsConfig(Map.of())))
                        .isEmpty();
    }

    // ------------------------------------------------------------------ test doubles

    private static CacheBuildConfig config(String defaultType, Map<String, String> typeByName) {
        Map<String, CacheBuildConfig.CacheTypeBuildConfig> byName = new HashMap<>(typeByName.size());
        typeByName.forEach((name, type) -> byName.put(name, () -> type));
        return new CacheBuildConfig() {
            @Override
            public String type() {
                return defaultType;
            }

            @Override
            public Map<String, CacheTypeBuildConfig> cacheTypeByName() {
                return byName;
            }
        };
    }

    private static NatsCachesBuildTimeConfig natsConfig(Map<String, Optional<String>> explicitBucketsByCache) {
        Map<String, NatsCachesBuildTimeConfig.Cache> caches = new HashMap<>(explicitBucketsByCache.size());
        explicitBucketsByCache.forEach((name, bucket) -> caches.put(name, new NatsCachesBuildTimeConfig.Cache() {
            @Override
            public Optional<String> bucket() {
                return bucket;
            }

            @Override
            public Optional<Duration> ttl() {
                return Optional.empty();
            }

            @Override
            public int history() {
                return 1;
            }
        }));
        return () -> caches;
    }

    private static Optional<String> bucket(String value) {
        return Optional.of(value);
    }
}
