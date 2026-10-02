package io.quarkiverse.nats.cache.deployment;

import jakarta.enterprise.context.ApplicationScoped;

import io.quarkus.cache.CacheResult;

/**
 * Test fixture: two caches whose names derive the same default NATS KV bucket — {@code mycache} and
 * {@code MYCACHE} both map to {@code MYCACHE} (case folding in the name-to-bucket derivation). Both names
 * are addressable by per-name properties, so every collision scenario (including explicit-bucket fixes)
 * can be expressed through configuration.
 */
@ApplicationScoped
public class CollisionService {

    @CacheResult(cacheName = "mycache")
    public String one(String id) {
        return "one-" + id;
    }

    @CacheResult(cacheName = "MYCACHE")
    public String two(String id) {
        return "two-" + id;
    }
}
