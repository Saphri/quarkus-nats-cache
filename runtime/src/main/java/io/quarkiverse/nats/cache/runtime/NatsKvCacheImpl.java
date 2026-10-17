package io.quarkiverse.nats.cache.runtime;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;

import org.jboss.logging.Logger;

import io.quarkiverse.nats.cache.runtime.codec.CacheValueException;
import io.quarkiverse.nats.cache.runtime.codec.KeyCodec;
import io.quarkiverse.nats.cache.runtime.codec.ValueCodec;
import io.quarkiverse.nats.cache.runtime.kv.CreateResult;
import io.quarkiverse.nats.cache.runtime.kv.KvBucket;
import io.quarkiverse.nats.cache.runtime.kv.ReadEntry;
import io.quarkus.cache.Cache;
import io.quarkus.cache.CacheException;
import io.quarkus.cache.runtime.AbstractCache;
import io.smallrye.mutiny.Uni;

/**
 * Implementation of the Quarkus cache API backed by a NATS JetStream KV bucket (design D1–D8).
 *
 * <p>Read path: {@code get}/{@code getAsync} first read the entry for the encoded key; a miss falls through to the
 * value loader. The read path fails open: an unreadable entry or an unreachable server also falls through to the
 * loader, and a failed write still returns the loaded value.
 * Concurrent loads of the same key share a single loader execution via the in-flight map (design D3). The loaded
 * value is stored with the universal envelope ({@code {data}}), so cached {@code null} results round-trip
 * losslessly.
 *
 * <p>Writes only ever create an entry (first write wins); an unreadable entry is purged before its reload is
 * stored.
 *
 * <p>Per-item expiration (design D6): {@code expiresAfter} is written as a server-side per-message TTL
 * ({@code Nats-TTL} header), so the server removes the entry when it elapses. NATS TTLs have whole-second
 * granularity: durations are rounded up to the next second. Entries without {@code expiresAfter} have no
 * per-item expiration (only the bucket's max age, if any, removes them). Buckets provisioned by this extension
 * allow per-message TTLs; a pre-existing bucket that does not rejects every write, and writes fail open (the loaded
 * value is returned uncached).
 *
 * <p>Invalidation: {@code invalidate(key)} purges the key (including its history); {@code invalidateAll()} purges
 * the whole bucket; {@code invalidateIf(predicate)} streams the bucket's keys, decodes each back to its original
 * Java object and purges the matches.
 *
 * <p>This class holds the cache policy only; how to talk to NATS lives behind {@link KvBucket}.
 */
public class NatsKvCacheImpl extends AbstractCache implements Cache {

    private static final Logger LOGGER = Logger.getLogger(NatsKvCacheImpl.class);

    private final NatsCacheInfo info;
    private final KvBucket bucket;
    private final KeyCodec keyCodec;
    private final ValueCodec valueCodec;

    /** Longest per-message TTL the KV client accepts (whole seconds as an {@code int}). */
    private static final Duration MAX_TTL = Duration.ofSeconds(Integer.MAX_VALUE);

    /**
     * In-flight loader per encoded key (design D3): concurrent loads of the same key share one execution.
     * Keyed by the <em>encoded</em> key so that distinct keys with equal {@code toString()} never collide.
     */
    private final Map<String, CompletableFuture<Object>> inFlight = new ConcurrentHashMap<>();

    /** Memoized bucket provisioning result (design D7); cleared on failure so the next operation retries. */
    private volatile CompletableFuture<Void> bucketReady;

    public NatsKvCacheImpl(NatsCacheInfo info, KvBucket bucket, KeyCodec keyCodec, ValueCodec valueCodec) {
        this.info = Objects.requireNonNull(info, "info must not be null");
        this.bucket = Objects.requireNonNull(bucket, "bucket must not be null");
        this.keyCodec = Objects.requireNonNull(keyCodec, "keyCodec must not be null");
        this.valueCodec = Objects.requireNonNull(valueCodec, "valueCodec must not be null");
    }

    @Override
    public String getName() {
        return info.name();
    }

    // ------------------------------------------------------------------ read path

    @Override
    public <K, V> Uni<V> get(K key, Function<K, V> valueLoader) {
        Objects.requireNonNull(valueLoader, "valueLoader must not be null");
        return doGet(key, k -> loadSync(key, valueLoader), null);
    }

    /**
     * Per-item expiration variant (design D6). The signature matches the upstream Quarkus {@code Cache} interface
     * exactly so that this method becomes an override as soon as the interface ships these methods; on older
     * Quarkus versions it is a plain extension method.
     */
    public <K, V> Uni<V> get(K key, Function<K, V> valueLoader, Duration expiresAfter) {
        Objects.requireNonNull(valueLoader, "valueLoader must not be null");
        return doGet(key, k -> loadSync(key, valueLoader), messageTtlOf(expiresAfter));
    }

    @Override
    public <K, V> Uni<V> getAsync(K key, Function<K, Uni<V>> valueLoader) {
        Objects.requireNonNull(valueLoader, "valueLoader must not be null");
        return doGet(key, valueLoader::apply, null);
    }

    /** Per-item expiration variant (design D6). See {@link #get(Object, Function, Duration)}. */
    public <K, V> Uni<V> getAsync(K key, Function<K, Uni<V>> valueLoader, Duration expiresAfter) {
        Objects.requireNonNull(valueLoader, "valueLoader must not be null");
        return doGet(key, valueLoader::apply, messageTtlOf(expiresAfter));
    }

    private <K, V> Uni<V> doGet(K key, Function<K, Uni<V>> loader, Duration messageTtl) {
        return Uni.createFrom().deferred(() -> {
            Objects.requireNonNull(key, NULL_KEYS_NOT_SUPPORTED_MSG);
            // Fail-fast (design D4): names the offending class and rejects over-length encodings; deferred turns
            // any synchronous exception into a failure signal for reactive consumers.
            String encoded = keyCodec.encode(key);
            return ensureBucket()
                    .chain(() -> bucket.get(encoded))
                    // Fail open: an unreachable server falls through to the loader like a miss, so a cache problem
                    // costs latency instead of failing the cached method. A transport error never purges.
                    .onFailure().recoverWithItem(failure -> {
                        LOGGER.warnf("NATS KV cache [%s]: read failed, falling back to the value loader: %s",
                                info.name(), failure.toString());
                        return ReadEntry.miss();
                    })
                    .chain(read -> {
                        if (read.isHit()) {
                            return decodeOrReload(read.payload(), key, encoded, loader, messageTtl);
                        }
                        if (read.isUnreadable()) {
                            LOGGER.warnf("NATS KV cache [%s]: stored entry is unreadable, replacing it with the "
                                    + "loaded value", info.name());
                            return purgeThenReload(key, encoded, loader, messageTtl);
                        }
                        return loadAndStore(key, encoded, loader, messageTtl);
                    });
        });
    }

    /**
     * Decodes a hit. A payload this application's codec cannot decode (corrupt, refused type, or written by an
     * incompatible class version) is an unreadable entry too, detected on this side of the seam.
     */
    private <K, V> Uni<V> decodeOrReload(byte[] payload, K key, String encoded, Function<K, Uni<V>> loader,
            Duration messageTtl) {
        Object value;
        try {
            value = valueCodec.decode(payload).data();
        } catch (CacheValueException e) {
            LOGGER.warnf("NATS KV cache [%s]: stored entry is unreadable, replacing it with the loaded value: %s",
                    info.name(), e.toString());
            return purgeThenReload(key, encoded, loader, messageTtl);
        }
        // null = cached-null result → suppresses the loader; unchecked: V is the caller's static type of the value
        return Uni.createFrom().item((V) value);
    }

    /**
     * Entries are only created, never overwritten: drop the unreadable entry first so the reloaded value can take
     * its place. A failed purge just leaves the value uncached.
     */
    private <K, V> Uni<V> purgeThenReload(K key, String encoded, Function<K, Uni<V>> loader, Duration messageTtl) {
        return bucket.purge(encoded).onFailure().recoverWithNull()
                .chain(() -> loadAndStore(key, encoded, loader, messageTtl));
    }

    private <K, V> Uni<V> loadSync(K key, Function<K, V> valueLoader) {
        // Synchronous loaders are blocking: run them on the worker pool, never on a Vert.x event loop (design D3).
        return Uni.createFrom().item(() -> valueLoader.apply(key)).runSubscriptionOn(NatsCacheWorkers.EXECUTOR);
    }

    /**
     * Validates {@code expiresAfter} as a per-message TTL. The KV client rounds it up to whole seconds (the server's
     * granularity), so an entry never expires early; durations beyond {@code Integer.MAX_VALUE} seconds (about 68
     * years), which the client refuses, are capped there.
     */
    static Duration messageTtlOf(Duration expiresAfter) {
        if (expiresAfter == null) {
            return null;
        }
        if (expiresAfter.isNegative() || expiresAfter.isZero()) {
            throw new IllegalArgumentException("expiresAfter must be positive");
        }
        return expiresAfter.compareTo(MAX_TTL) > 0 ? MAX_TTL : expiresAfter;
    }

    // ------------------------------------------------------------------ stampede protection (design D3)

    private <K, V> Uni<V> loadAndStore(K key, String encoded, Function<K, Uni<V>> loader, Duration messageTtl) {
        return Uni.createFrom().deferred(() -> {
            CompletableFuture<Object> pending = new CompletableFuture<>();
            CompletableFuture<Object> shared = inFlight.putIfAbsent(encoded, pending);
            if (shared == null) {
                // Winner: run load → store detached from any caller's subscription, so that cancelling one caller
                // (client disconnect, upstream timeout) never cancels the shared load or strands the waiters.
                // The map entry is removed before the future settles, so a settled future is never handed out.
                shared = pending;
                Uni.createFrom().deferred(() -> loader.apply(key))
                        .flatMap(value -> store(encoded, value, messageTtl))
                        .subscribe().with(
                                value -> {
                                    inFlight.remove(encoded, pending);
                                    pending.complete(value); // may be null (cached-null): waiters get a null item
                                },
                                failure -> {
                                    inFlight.remove(encoded, pending);
                                    pending.completeExceptionally(failure);
                                });
            }
            // Every caller (winner included) awaits a copy: Mutiny cancels the CompletableFuture it subscribes to
            // when the subscriber cancels, and that must only ever affect this caller.
            return Uni.createFrom().completionStage(shared.copy()).map(o -> (V) o);
        });
    }

    /**
     * Writes a new entry, optionally with a per-message TTL (design D6). Entries are only ever created, never
     * updated: a live entry is kept until it expires or is invalidated, so a write for a key that already holds a
     * value is refused (first write wins). A create also succeeds over a delete, purge or expiry marker, which covers
     * expired and invalidated keys.
     */
    private <V> Uni<V> store(String encoded, V value, Duration messageTtl) {
        // Encoding errors are programming errors (unserializable value) and fail the call; a failed write is a
        // cache outage and does not (fail open): the loaded value is returned, just not cached.
        byte[] payload = valueCodec.encode(value);
        return bucket.create(encoded, payload, messageTtl)
                .onItemOrFailure().transform((result, failure) -> {
                    if (failure != null) {
                        LOGGER.warnf("NATS KV cache [%s]: write failed, returning the loaded value uncached: %s",
                                info.name(), failure.toString());
                    } else if (result == CreateResult.ALREADY_CACHED) {
                        // Another writer (typically another instance) cached the key first: first write wins.
                        LOGGER.debugf("NATS KV cache [%s]: entry already cached by another writer", info.name());
                    }
                    return value;
                });
    }

    // ------------------------------------------------------------------ invalidation

    @Override
    public Uni<Void> invalidate(Object key) {
        return Uni.createFrom().deferred(() -> {
            Objects.requireNonNull(key, NULL_KEYS_NOT_SUPPORTED_MSG);
            String encoded = keyCodec.encode(key);
            // purge (not delete): removes the key and all of its history; a no-op for absent keys.
            return ensureBucket().chain(() -> bucket.purge(encoded));
        });
    }

    @Override
    public Uni<Void> invalidateAll() {
        // Provision first: purging a bucket that does not exist yet is an API error, and provisioning it (empty)
        // before the purge is a harmless no-op that keeps invalidateAll idempotent from first use. Invalidation is
        // deliberately not fail-open: a failed purge fails the call.
        return ensureBucket()
                .chain(() -> bucket.purgeAll().onFailure().transform(
                        e -> new CacheException("Failed to purge NATS KV bucket '" + info.bucket() + "'", e)));
    }

    @Override
    public Uni<Void> invalidateIf(Predicate<Object> predicate) {
        Objects.requireNonNull(predicate, "predicate must not be null");
        return ensureBucket()
                .chain(() -> bucket.keys()
                        .map(this::decodeStoredKey) // undecodable stored key → WARN + skip, never aborts
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .filter(pair -> predicate.test(pair.decoded()))
                        .onItem().call(pair -> bucket.purge(pair.encoded()))
                        .collect().asList())
                .replaceWithVoid();
    }

    private Optional<StoredKey> decodeStoredKey(String encoded) {
        try {
            return Optional.of(new StoredKey(encoded, keyCodec.decode(encoded)));
        } catch (RuntimeException e) {
            LOGGER.warnf("Skipping stored key that cannot be decoded by the NATS KV cache codec: %s", encoded);
            return Optional.empty(); // a Mutiny mapper must not return null
        }
    }

    // ------------------------------------------------------------------ bucket provisioning (design D7)

    private Uni<Void> ensureBucket() {
        // copy(): a cancelled caller must not cancel the shared provisioning future of every other caller.
        return Uni.createFrom().completionStage(readyFuture().copy());
    }

    private CompletableFuture<Void> readyFuture() {
        CompletableFuture<Void> f = bucketReady;
        if (f != null) {
            return f;
        }
        synchronized (this) {
            if (bucketReady != null) {
                return bucketReady;
            }
            // add-if-absent: an existing bucket (e.g. created externally) is left unmodified.
            CompletableFuture<Void> attempt = bucket.provision().convert().toCompletableFuture();
            // Memoize before registering the callback: the attempt may already have failed, and the callback then
            // runs right away and must find it to clear it.
            bucketReady = attempt;
            attempt.whenComplete((v, error) -> {
                if (error != null) {
                    synchronized (this) {
                        if (bucketReady == attempt) {
                            bucketReady = null; // clear so the next cache operation retries provisioning
                        }
                    }
                    LOGGER.warnf("Failed to provision NATS KV bucket '%s'; will retry on the next cache operation",
                            info.bucket());
                }
            });
            return attempt;
        }
    }

    // ------------------------------------------------------------------ internal types

    /** A stored key together with its decoded form, for {@link #invalidateIf(Predicate)}. */
    private record StoredKey(String encoded, Object decoded) {
    }
}
