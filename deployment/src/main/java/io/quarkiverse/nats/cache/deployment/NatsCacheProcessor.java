package io.quarkiverse.nats.cache.deployment;

import java.util.Set;

import org.jboss.jandex.ClassInfo;

import io.quarkiverse.nats.cache.runtime.NatsCacheBuildRecorder;
import io.quarkiverse.nats.cache.runtime.NatsCachesBuildTimeConfig;
import io.quarkus.arc.deployment.ValidationPhaseBuildItem;
import io.quarkus.cache.DefaultCacheKey;
import io.quarkus.cache.deployment.CacheManagerInfoBuildItem;
import io.quarkus.cache.deployment.CacheNamesBuildItem;
import io.quarkus.cache.runtime.CacheBuildConfig;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.annotations.ExecutionTime;
import io.quarkus.deployment.annotations.Record;
import io.quarkus.deployment.builditem.ApplicationIndexBuildItem;
import io.quarkus.deployment.builditem.FeatureBuildItem;
import io.quarkus.deployment.builditem.nativeimage.ReflectiveClassBuildItem;

/**
 * Deployment build steps for the NATS KV cache backend.
 *
 * <p>Follows the {@code InfinispanCacheProcessor} pattern: a {@code @Record(RUNTIME_INIT)} step produces the
 * {@link CacheManagerInfoBuildItem} whose supplier is resolved lazily by the core cache extension when it
 * builds the {@code CacheManager} bean, and plain build steps handle native-image reflection and build-time
 * validation.
 */
public class NatsCacheProcessor {

    @BuildStep
    FeatureBuildItem feature() {
        return new FeatureBuildItem("nats-cache");
    }

    /**
     * Registers the NATS KV backend for cache type {@code nats} (design D1, D2). The core cache extension
     * collects all {@link CacheManagerInfoBuildItem}s and asks each one's {@code supports(context)} whether it
     * handles the resolved type of the requested caches; ours answers for {@code "nats"} only, so it coexists
     * with the Caffeine backend (task 4.3).
     */
    @BuildStep
    @Record(value = ExecutionTime.RUNTIME_INIT)
    CacheManagerInfoBuildItem cacheManagerInfo(NatsCacheBuildRecorder recorder) {
        return new CacheManagerInfoBuildItem(recorder.getCacheManagerSupplier());
    }

    /**
     * Native-image reflection for the codec (design D8).
     *
     * <p>{@code KeyCodec} reads the private {@code cacheName} field of {@link DefaultCacheKey} reflectively.
     *
     * <p>Cached values are arbitrary application types whose exact runtime class is only known at run time
     * (the codec embeds it as a Jackson {@code @class} property and deserializes with default typing). Quarkus
     * only auto-registers types it can infer statically (e.g. REST method signatures), so an unregistered POJO
     * would silently serialize to an empty object in native mode. We therefore register every <em>application</em>
     * class (constructors, methods and fields) for reflection whenever at least one {@code nats}-typed cache
     * exists. Third-party library types still need explicit registration ({@code @RegisterForReflection} or
     * config), like any other dynamic Jackson usage in native mode.
     */
    @BuildStep
    void nativeImage(ApplicationIndexBuildItem appIndex, CacheNamesBuildItem cacheNames,
            CacheBuildConfig cacheBuildConfig, BuildProducer<ReflectiveClassBuildItem> producer) {
        producer.produce(ReflectiveClassBuildItem.builder(DefaultCacheKey.class)
                .reason(getClass().getName())
                .fields(true)
                .build());

        Set<String> names = cacheNames.getNames();
        boolean hasNatsCache = names.stream()
                .anyMatch(n -> NatsCacheBuildRecorder.CACHE_TYPE.equals(BucketCollisionValidator.typeOf(n, cacheBuildConfig)));
        if (!hasNatsCache) {
            return; // no nats-backed cache: nothing is (de)serialized by the codec
        }
        for (ClassInfo appClass : appIndex.getIndex().getKnownClasses()) {
            // Interfaces and annotations are never instantiated by Jackson default typing (the embedded
            // type id always names a concrete class); registering them only bloats the native image.
            if (appClass.isInterface() || appClass.isAnnotation()) {
                continue;
            }
            producer.produce(ReflectiveClassBuildItem.builder(appClass.name().toString())
                    .reason("quarkus-nats-cache: cached values are arbitrary application types, "
                            + "reconstructed reflectively by Jackson default typing")
                    .constructors(true)
                    .methods(true)
                    .fields(true)
                    .build());
        }
    }

    /**
     * Build-time bucket collision validation (design D8): two {@code nats}-typed caches that would end up on
     * the same bucket because of the default name-to-bucket derivation fail the build, naming both caches and
     * pointing at the explicit bucket property. Deliberate sharing via explicit {@code bucket=} values is allowed.
     *
     * <p>Reported as Quarkus validation errors (standard diagnostics, grouped with any other validation
     * failure) instead of a raw {@code DeploymentException}; producing build items also keeps this step in
     * the build graph without an artificial always-run marker.
     */
    @BuildStep
    void validateBucketCollisions(CacheNamesBuildItem cacheNames, CacheBuildConfig cacheBuildConfig,
            NatsCachesBuildTimeConfig natsConfig,
            BuildProducer<ValidationPhaseBuildItem.ValidationErrorBuildItem> validationErrors) {
        for (String message : BucketCollisionValidator.findCollisions(cacheNames.getNames(), cacheBuildConfig, natsConfig)) {
            validationErrors.produce(new ValidationPhaseBuildItem.ValidationErrorBuildItem(new IllegalStateException(message)));
        }
    }
}
