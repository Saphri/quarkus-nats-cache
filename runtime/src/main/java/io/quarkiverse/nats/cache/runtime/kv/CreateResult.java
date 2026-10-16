package io.quarkiverse.nats.cache.runtime.kv;

/** Outcome of {@link KvBucket#create}: entries are create-only, so the first write wins. */
public enum CreateResult {
    /** The entry was written. */
    CREATED,
    /** The key already holds a live entry (written by another writer first); nothing was written. */
    ALREADY_CACHED
}
