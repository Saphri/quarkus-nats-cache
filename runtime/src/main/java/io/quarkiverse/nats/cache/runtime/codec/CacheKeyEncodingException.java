package io.quarkiverse.nats.cache.runtime.codec;

import io.quarkus.cache.CacheException;

/**
 * Thrown when a cache key cannot be encoded into (or reconstructed from) the NATS KV subject-safe form.
 * The message always names the offending key class so applications can fix their key types quickly.
 */
public class CacheKeyEncodingException extends CacheException {

    public CacheKeyEncodingException(String message) {
        super(message, null);
    }

    public CacheKeyEncodingException(String message, Throwable cause) {
        super(message, cause);
    }
}
