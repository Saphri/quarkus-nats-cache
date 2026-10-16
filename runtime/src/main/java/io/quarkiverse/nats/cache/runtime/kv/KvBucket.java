package io.quarkiverse.nats.cache.runtime.kv;

import java.time.Duration;
import java.util.Optional;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

/**
 * KV bucket access: the seam between the cache policy ({@code NatsKvCacheImpl}) and the mechanics of talking to
 * one NATS KV bucket. Keys cross the seam already encoded ({@code KeyCodec} stays on the cache side); payloads are
 * opaque bytes. Implementations translate transport quirks into the clean outcomes below; a failure signal always
 * means a real error.
 */
public interface KvBucket {

    /**
     * Reads the entry for a key.
     *
     * @return the stored payload (possibly empty), or an empty optional on a miss
     */
    Uni<Optional<byte[]>> get(String encodedKey);

    /**
     * Creates an entry; never overwrites a live one (first write wins). A create succeeds over a delete, purge or
     * expiry marker.
     *
     * @param perItemTtlOrNull the per-message TTL, or {@code null} for an entry that only the bucket's own max age
     *        expires
     * @return {@link CreateResult#ALREADY_CACHED} when the key already holds a live entry
     */
    Uni<CreateResult> create(String encodedKey, byte[] payload, Duration perItemTtlOrNull);

    /** Purges a key and its history; a no-op for an absent key. */
    Uni<Void> purge(String encodedKey);

    /** Streams the encoded keys currently stored in the bucket. */
    Multi<String> keys();

    /** Creates the bucket if it does not exist (add-if-absent); an existing bucket is left unmodified. */
    Uni<Void> provision();

    /** Purges every entry of the bucket. The bucket must exist. */
    Uni<Void> purgeAll();
}
