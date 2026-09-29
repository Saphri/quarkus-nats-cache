package io.quarkiverse.nats.cache.runtime;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import org.jboss.logging.Logger;

import io.quarkiverse.reactive.messaging.nats.jetstream.client.Client;
import io.quarkus.arc.Arc;
import io.quarkus.cache.Cache;
import io.quarkus.cache.CacheManager;
import io.quarkus.cache.CacheManagerInfo;
import io.quarkus.cache.runtime.CacheManagerImpl;
import io.quarkus.runtime.annotations.Recorder;

/**
 * Records the {@link CacheManagerInfo} for the NATS KV cache backend (design D1, D2).
 *
 * <p>Follows the Infinispan cache extension pattern: the build-time config mapping is captured here and the
 * per-cache implementations are built lazily inside the returned supplier. The {@link Client} bean of the
 * quarkiverse nats-jetstream extension is resolved through Arc at supplier invocation time (the default data
 * source's client carries CDI {@code @Default}, so an unqualified lookup selects it).
 */
@Recorder
public class NatsCacheBuildRecorder {

    private static final Logger LOGGER = Logger.getLogger(NatsCacheBuildRecorder.class);

    /** The cache type string this backend answers to ({@code quarkus.cache.type} / per-name overrides). */
    public static final String CACHE_TYPE = "nats";

    private final NatsCachesBuildTimeConfig config;

    public NatsCacheBuildRecorder(NatsCachesBuildTimeConfig config) {
        this.config = config;
    }

    /**
     * @return the {@link CacheManagerInfo} matching cache type {@code nats}
     */
    public CacheManagerInfo getCacheManagerSupplier() {
        return new CacheManagerInfo() {
            @Override
            public boolean supports(CacheManagerInfo.Context context) {
                return context.cacheEnabled() && CACHE_TYPE.equals(context.cacheType());
            }

            @Override
            public Supplier<CacheManager> get(CacheManagerInfo.Context context) {
                // Only the names of caches configured with type "nats" reach this supplier.
                Set<String> cacheNames = context.cacheNames();
                return () -> {
                    Client client = Arc.container().instance(Client.class).get();
                    Map<String, Cache> caches = new HashMap<>(cacheNames.size() + 1);
                    for (String name : cacheNames) {
                        NatsCacheInfo info = infoFor(name);
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debugf("Building NATS KV cache [%s] on bucket [%s]", name, info.bucket());
                        }
                        caches.put(name, new NatsKvCacheImpl(info, client));
                    }
                    return new CacheManagerImpl(caches);
                };
            }
        };
    }

    private NatsCacheInfo infoFor(String name) {
        NatsCachesBuildTimeConfig.Cache cache = config.caches().get(name);
        Optional<String> bucket = cache != null ? cache.bucket() : Optional.empty();
        Optional<Duration> ttl = cache != null ? cache.ttl() : Optional.empty();
        int history = cache != null ? cache.history() : 1;
        return NatsCacheInfo.of(name, bucket, ttl, history);
    }
}
