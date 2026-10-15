package io.quarkiverse.nats.cache.runtime;

import static io.nats.client.support.NatsJetStreamConstants.JS_WRONG_LAST_SEQUENCE;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;

import org.jboss.logging.Logger;

import io.nats.client.JetStreamApiException;
import io.nats.client.support.NatsKeyValueUtil;
import io.quarkiverse.nats.cache.runtime.codec.CacheValueException;
import io.quarkiverse.nats.cache.runtime.codec.KeyCodec;
import io.quarkiverse.nats.cache.runtime.codec.ValueCodec;
import io.quarkiverse.reactive.messaging.nats.jetstream.client.Client;
import io.quarkiverse.reactive.messaging.nats.jetstream.client.store.KeyValue;
import io.quarkiverse.reactive.messaging.nats.jetstream.client.store.api.KeyValueEntry;
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
 * granularity: durations are rounded up to the next second. Entries without {@code expiresAfter} carry the
 * longest TTL the client accepts (about 68 years). Buckets provisioned by this extension allow per-message TTLs; a
 * pre-existing bucket that does not rejects every write, and writes fail open (the loaded value is returned
 * uncached).
 *
 * <p>Invalidation: {@code invalidate(key)} purges the key (including its history); {@code invalidateAll()} purges
 * the whole KV stream through raw JNats; {@code invalidateIf(predicate)} streams the bucket's keys, decodes each
 * back to its original Java object and purges the matches.
 */
public class NatsKvCacheImpl extends AbstractCache implements Cache {

    private static final Logger LOGGER = Logger.getLogger(NatsKvCacheImpl.class);

    private final NatsCacheInfo info;
    private final KeyValue kv;
    private final Client client;

    /** Longest per-message TTL the KV client accepts (whole seconds as an {@code int}). */
    private static final Duration MAX_TTL = Duration.ofSeconds(Integer.MAX_VALUE);

    /** Lifetime of the marker a purge leaves behind (the server minimum). */
    static final Duration PURGE_MARKER_TTL = Duration.ofSeconds(1);

    /**
     * In-flight loader per encoded key (design D3): concurrent loads of the same key share one execution.
     * Keyed by the <em>encoded</em> key so that distinct keys with equal {@code toString()} never collide.
     */
    private final Map<String, CompletableFuture<Object>> inFlight = new ConcurrentHashMap<>();

    /** Memoized bucket provisioning result (design D7); cleared on failure so the next operation retries. */
    private volatile CompletableFuture<Void> bucketReady;

    public NatsKvCacheImpl(NatsCacheInfo info, Client client) {
        this.info = Objects.requireNonNull(info, "info must not be null");
        this.client = Objects.requireNonNull(client, "client must not be null");
        this.kv = client.keyValue(info.bucket());
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
            String encoded = KeyCodec.encode(key);
            return ensureBucket()
                    .chain(() -> kv.get(encoded))
                    .onItem().ifNull().failWith(CacheMiss::new) // quarkiverse 3.39.x reports a miss as a null item
                    .flatMap(this::readEntry) // decoded value is an opaque Object; cast once, at the end
                    // Fail open: a miss, an unreadable entry (corrupt, refused type, or written by an incompatible
                    // class version) and an unreachable server all fall through to the loader, so a cache problem
                    // costs latency instead of failing the cached method.
                    .onFailure().recoverWithUni(failure -> {
                        if (!(failure instanceof CacheMiss)) {
                            LOGGER.warnf("NATS KV cache [%s]: read failed, falling back to the value loader: %s",
                                    info.name(), failure.toString());
                        }
                        if (failure instanceof CacheValueException) {
                            // Entries are only created, never overwritten: drop the unreadable entry first so the
                            // reloaded value can take its place. A failed purge just leaves the value uncached.
                            return purge(encoded).onFailure().recoverWithNull()
                                    .chain(() -> loadAndStore(key, encoded, loader, messageTtl));
                        }
                        return loadAndStore(key, encoded, loader, messageTtl);
                    })
                    .map(v -> (V) v); // unchecked: V is the caller's static type of the cached value
        });
    }

    private Uni<Object> readEntry(KeyValueEntry entry) {
        byte[] payload = entry.value().orElse(null);
        if (payload == null || payload.length == 0) {
            throw new CacheValueException("Stored cache value is empty"); // foreign data → unreadable
        }
        ValueCodec.Envelope envelope = ValueCodec.decode(payload); // undecodable → CacheValueException → reload
        return Uni.createFrom().item(envelope.data()); // null item = cached-null result → suppresses the loader
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

    private <V> Uni<V> store(String encoded, V value, Duration messageTtl) {
        // Encoding errors are programming errors (unserializable value) and fail the call; a failed write is a
        // cache outage and does not (fail open): the loaded value is returned, just not cached.
        byte[] payload = ValueCodec.encode(value);
        return create(encoded, payload, messageTtl)
                .onItemOrFailure().transform((ignored, failure) -> {
                    if (isAlreadyCached(failure)) {
                        // Another writer (typically another instance) cached the key first: first write wins.
                        LOGGER.debugf("NATS KV cache [%s]: entry already cached by another writer", info.name());
                    } else if (failure != null) {
                        LOGGER.warnf("NATS KV cache [%s]: write failed, returning the loaded value uncached: %s",
                                info.name(), failure.toString());
                    }
                    return value;
                });
    }

    /**
     * Writes a new entry, optionally with a per-message TTL (design D6). Entries are only ever created, never
     * updated: a live entry is kept until it expires or is invalidated, so a write for a key that already holds a
     * value is refused (first write wins). A create also succeeds over a delete, purge or expiry marker, which covers
     * expired and invalidated keys.
     *
     * <p>The quarkiverse KeyValue only offers create with a TTL, so an entry without {@code expiresAfter} is written
     * with the longest TTL the client accepts; the bucket's own max age (if any) still applies.
     */
    private Uni<?> create(String encoded, byte[] payload, Duration messageTtl) {
        return kv.create(encoded, payload, messageTtl != null ? messageTtl : MAX_TTL);
    }

    private static boolean isAlreadyCached(Throwable failure) {
        for (Throwable t = failure; t != null; t = t.getCause()) {
            if (t instanceof JetStreamApiException e) {
                return e.getApiErrorCode() == JS_WRONG_LAST_SEQUENCE;
            }
        }
        return false;
    }

    /**
     * Purges a key (and its history). The purge marker carries a short TTL so the server removes it too: nothing
     * here reads markers, and without a TTL every invalidated key would leave one behind for good. A bucket that
     * does not allow per-message TTLs (created externally) refuses that, so fall back to a plain purge.
     */
    private Uni<Void> purge(String encoded) {
        return kv.purge(encoded, PURGE_MARKER_TTL).onFailure().recoverWithUni(() -> kv.purge(encoded));
    }

    // ------------------------------------------------------------------ invalidation

    @Override
    public Uni<Void> invalidate(Object key) {
        return Uni.createFrom().deferred(() -> {
            Objects.requireNonNull(key, NULL_KEYS_NOT_SUPPORTED_MSG);
            String encoded = KeyCodec.encode(key);
            // purge (not delete): removes the key and all of its history; JNats purge is a no-op for absent keys.
            return ensureBucket().chain(() -> purge(encoded)).replaceWithVoid();
        });
    }

    @Override
    public Uni<Void> invalidateAll() {
        // JNats has no purgeAll: purge the whole KV stream. Blocking raw-JNats call → worker pool (design D3).
        // ensureBucket first: purging a stream that does not exist yet is an API error, and provisioning it
        // (empty) before the purge is a harmless no-op that keeps invalidateAll idempotent from first use.
        return ensureBucket()
                .chain(() -> Uni.createFrom().<Void>item(() -> {
                    try {
                        client.nativeConnection().jetStreamManagement()
                                .purgeStream(NatsKeyValueUtil.toStreamName(info.bucket()));
                    } catch (Exception e) {
                        throw new CacheException("Failed to purge NATS KV bucket '" + info.bucket() + "'", e);
                    }
                    return null;
                }).runSubscriptionOn(NatsCacheWorkers.EXECUTOR))
                .replaceWithVoid();
    }

    @Override
    public Uni<Void> invalidateIf(Predicate<Object> predicate) {
        Objects.requireNonNull(predicate, "predicate must not be null");
        return ensureBucket()
                .chain(() -> kv.keys()
                        .map(NatsKvCacheImpl::decodeStoredKey) // undecodable stored key → WARN + skip, never aborts
                        .filter(Objects::nonNull)
                        .filter(pair -> predicate.test(pair.decoded()))
                        .onItem().call(pair -> purge(pair.encoded()))
                        .collect().asList())
                .replaceWithVoid();
    }

    private static StoredKey decodeStoredKey(String encoded) {
        try {
            return new StoredKey(encoded, KeyCodec.decode(encoded));
        } catch (RuntimeException e) {
            LOGGER.warnf("Skipping stored key that cannot be decoded by the NATS KV cache codec: %s", encoded);
            return null;
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
            if (bucketReady == null) {
                // add-if-absent: an existing bucket (e.g. created externally) is left unmodified.
                CompletableFuture<Void> attempt = client.keyValueManagement()
                        .addIfAbsent(info.keyValueConfiguration())
                        .convert().toCompletableFuture();
                attempt.whenComplete((v, error) -> {
                    if (error != null) {
                        bucketReady = null; // clear so the next cache operation retries provisioning
                        LOGGER.warnf("Failed to provision NATS KV bucket '%s'; will retry on the next cache operation",
                                info.bucket());
                    }
                });
                bucketReady = attempt;
            }
            return bucketReady;
        }
    }

    // ------------------------------------------------------------------ internal types

    /**
     * Internal failure marker distinguishing "no usable entry" (miss) from real errors.
     * Misses are a hot-path event, so stack-trace filling and suppression are disabled to avoid the
     * allocation cost on every miss.
     */
    private static final class CacheMiss extends RuntimeException {

        private CacheMiss() {
            super(null, null, false, false);
        }
    }

    /** A stored key together with its decoded form, for {@link #invalidateIf(Predicate)}. */
    private record StoredKey(String encoded, Object decoded) {
    }
}
