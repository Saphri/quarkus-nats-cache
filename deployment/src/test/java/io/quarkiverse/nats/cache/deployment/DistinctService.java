package io.quarkiverse.nats.cache.deployment;

import jakarta.enterprise.context.ApplicationScoped;

import io.quarkus.cache.CacheResult;

/**
 * Test fixture: two caches whose names derive distinct default NATS KV buckets
 * ({@code ALPHA-CACHE} and {@code BETA-CACHE}).
 */
@ApplicationScoped
public class DistinctService {

    @CacheResult(cacheName = "alpha-cache")
    public String one(String id) {
        return "one-" + id;
    }

    @CacheResult(cacheName = "beta-cache")
    public String two(String id) {
        return "two-" + id;
    }
}
