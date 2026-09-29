package io.quarkiverse.nats.cache.deployment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import jakarta.enterprise.inject.spi.DeploymentException;

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
    void derivedBucketCollisionFailsNamingBothCaches() {
        assertThatThrownBy(() -> BucketCollisionValidator.validate(
                COLLIDING_NAMES, config("nats", Map.of()), natsConfig(Map.of())))
                .isInstanceOf(DeploymentException.class)
                .hasMessageContainingAll("MY_CACHE", "'my.cache'", "'my_cache'")
                .hasMessageContaining("quarkus.nats-cache.caches.<name>.bucket");
    }

    @Test
    void explicitlySharedBucketIsAllowed() {
        assertThatCode(() -> BucketCollisionValidator.validate(
                Set.of("alpha", "beta"), config("nats", Map.of()),
                natsConfig(Map.of("alpha", bucket("SHARED"), "beta", bucket("SHARED")))))
                .doesNotThrowAnyException();
    }

    @Test
    void explicitBucketCollidingWithDerivedOneFails() {
        // 'x' explicitly chooses Y_CACHE while 'y_cache' derives to the same bucket.
        assertThatThrownBy(() -> BucketCollisionValidator.validate(
                Set.of("x", "y_cache"), config("nats", Map.of()),
                natsConfig(Map.of("x", bucket("Y_CACHE")))))
                .isInstanceOf(DeploymentException.class)
                .hasMessageContainingAll("Y_CACHE", "'x'", "'y_cache'");
    }

    @Test
    void nonNatsCachesAreIgnored() {
        // Same derived-bucket pattern, but the caches are of another backend type.
        assertThatCode(() -> BucketCollisionValidator.validate(
                COLLIDING_NAMES, config("caffeine", Map.of()), natsConfig(Map.of())))
                .doesNotThrowAnyException();
    }

    @Test
    void perNameTypeOverrideExcludesCacheFromValidation() {
        // 'my_cache' is overridden to another backend, so only one nats cache remains on MY_CACHE.
        Map<String, String> overrides = new HashMap<>();
        overrides.put("my_cache", "caffeine");
        assertThatCode(() -> BucketCollisionValidator.validate(
                COLLIDING_NAMES, config("nats", overrides), natsConfig(Map.of())))
                .doesNotThrowAnyException();
    }

    @Test
    void distinctDerivedBucketsAreFine() {
        assertThatCode(() -> BucketCollisionValidator.validate(
                Set.of("alpha", "beta"), config("nats", Map.of()), natsConfig(Map.of())))
                .doesNotThrowAnyException();
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
