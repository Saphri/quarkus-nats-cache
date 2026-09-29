package io.quarkiverse.nats.cache.runtime.codec;

import io.quarkus.cache.CacheException;

/**
 * Thrown when a stored payload cannot be decoded into the cache value envelope
 * (e.g. a hand-written entry in the bucket, or data written by an incompatible version).
 */
public class CacheValueException extends CacheException {

    public CacheValueException(String message) {
        super(message, null);
    }

    public CacheValueException(String message, Throwable cause) {
        super(message, cause);
    }
}
