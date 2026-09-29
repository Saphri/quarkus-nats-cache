package io.quarkiverse.nats.cache.deployment;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import jakarta.enterprise.inject.spi.DeploymentException;

import io.quarkiverse.nats.cache.runtime.NatsCacheBuildRecorder;
import io.quarkiverse.nats.cache.runtime.NatsCachesBuildTimeConfig;
import io.quarkiverse.nats.cache.runtime.NatsCacheInfo;
import io.quarkus.cache.runtime.CacheBuildConfig;

/**
 * Build-time bucket collision validation (design D8).
 *
 * <p>The default name-to-bucket derivation ({@link NatsCacheInfo#defaultBucket(String)}) is not injective: for
 * example {@code my.cache} and {@code my_cache} both derive to {@code MY_CACHE}. When two {@code nats}-typed
 * caches end up on the same bucket because of that derivation, the build fails naming both caches and pointing
 * at the explicit bucket property. Two caches that <em>explicitly</em> configure the same bucket are allowed:
 * sharing a bucket is then a deliberate choice.
 */
final class BucketCollisionValidator {

    private BucketCollisionValidator() {
    }

    /**
     * @param cacheNames the cache names referenced by the application (from {@code CacheNamesBuildItem})
     * @param cacheBuildConfig the core cache build-time config ({@code quarkus.cache.type} + per-name overrides)
     * @param natsConfig this extension's build-time config ({@code quarkus.nats-cache.caches.<name>.*})
     * @throws DeploymentException if two {@code nats} caches derive the same bucket
     */
    static void validate(Set<String> cacheNames, CacheBuildConfig cacheBuildConfig, NatsCachesBuildTimeConfig natsConfig) {
        Map<String, Set<String>> bucketToCaches = new LinkedHashMap<>();
        Map<String, Boolean> explicitByCache = new LinkedHashMap<>();

        for (String name : cacheNames) {
            if (!NatsCacheBuildRecorder.CACHE_TYPE.equals(typeOf(name, cacheBuildConfig))) {
                continue; // not a nats cache: another backend's rules apply
            }
            NatsCachesBuildTimeConfig.Cache settings = natsConfig.caches().get(name);
            Optional<String> explicitBucket = settings != null ? settings.bucket() : Optional.empty();
            String bucket = explicitBucket.orElseGet(() -> NatsCacheInfo.defaultBucket(name));
            bucketToCaches.computeIfAbsent(bucket, b -> new LinkedHashSet<>()).add(name);
            explicitByCache.put(name, explicitBucket.isPresent());
        }

        for (Map.Entry<String, Set<String>> entry : bucketToCaches.entrySet()) {
            Set<String> caches = entry.getValue();
            if (caches.size() < 2) {
                continue;
            }
            boolean allExplicit = caches.stream().allMatch(n -> Boolean.TRUE.equals(explicitByCache.get(n)));
            if (allExplicit) {
                continue; // explicitly shared bucket: deliberate
            }
            String names = caches.stream().sorted().map(n -> "'" + n + "'").collect(Collectors.joining(" and "));
            StringBuilder message = new StringBuilder(256);
            message.append("NATS KV bucket '").append(entry.getKey())
                    .append("' would be shared by caches ").append(names)
                    .append(", but at least one of them uses the default (derived) bucket name, which is ambiguous. ")
                    .append("Set an explicit bucket for each of these caches via quarkus.nats-cache.caches.<name>.bucket.");
            if (caches.stream().anyMatch(n -> n.contains("."))) {
                message.append(" Note: a cache name containing '.' cannot be addressed by per-name properties at all ")
                        .append("(smallrye-config property-naming limitation; it also affects the core quarkus.cache.<name>.type override) — rename such a cache instead.");
            }
            throw new DeploymentException(message.toString());
        }
    }

    /**
     * Resolves the cache type of a single cache name exactly like the core cache extension does: the
     * per-name {@code quarkus.cache.<name>.type} override wins, otherwise the default {@code quarkus.cache.type}.
     */
    static String typeOf(String name, CacheBuildConfig config) {
        CacheBuildConfig.CacheTypeBuildConfig byName = config.cacheTypeByName().get(name);
        if (byName != null && byName.type() != null && !byName.type().isEmpty()) {
            return byName.type();
        }
        return config.type();
    }
}
