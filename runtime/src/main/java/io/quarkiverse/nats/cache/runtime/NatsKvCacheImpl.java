package io.quarkiverse.nats.cache.runtime;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;

import org.jboss.logging.Logger;

import io.nats.client.support.NatsKeyValueUtil;
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
 * <p>Read path: {@code get}/{@code getAsync} first read the entry for the encoded key; a miss (or a logically
 * expired entry, design D6) falls through to the value loader. Concurrent loads of the same key share a single
 * loader execution via the in-flight map (design D3). The loaded value is stored with the universal envelope
 * ({@code {exp?, data}}), so cached {@code null} results and per-item expirations round-trip losslessly.
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
        return doGet(key, k -> loadSync(key, valueLoader), expirationOf(expiresAfter));
    }

    @Override
    public <K, V> Uni<V> getAsync(K key, Function<K, Uni<V>> valueLoader) {
        Objects.requireNonNull(valueLoader, "valueLoader must not be null");
        return doGet(key, valueLoader::apply, null);
    }

    /** Per-item expiration variant (design D6). See {@link #get(Object, Function, Duration)}. */
    public <K, V> Uni<V> getAsync(K key, Function<K, Uni<V>> valueLoader, Duration expiresAfter) {
        Objects.requireNonNull(valueLoader, "valueLoader must not be null");
        return doGet(key, valueLoader::apply, expirationOf(expiresAfter));
    }

    private <K, V> Uni<V> doGet(K key, Function<K, Uni<V>> loader, Long expiresAtEpochMillis) {
        return Uni.createFrom().deferred(() -> {
            Objects.requireNonNull(key, NULL_KEYS_NOT_SUPPORTED_MSG);
            // Fail-fast (design D4): names the offending class and rejects over-length encodings; deferred turns
            // any synchronous exception into a failure signal for reactive consumers.
            String encoded = KeyCodec.encode(key);
            return ensureBucket()
                    .chain(() -> kv.get(encoded))
                    .onItem().ifNull().failWith(CacheMiss::new) // quarkiverse 3.39.x reports a miss as a null item
                    .flatMap(this::readEntry) // decoded value is an opaque Object; cast once, at the end
                    .onFailure(CacheMiss.class)
                    .recoverWithUni(() -> loadAndStore(key, encoded, loader, expiresAtEpochMillis))
                    .map(v -> (V) v); // unchecked: V is the caller's static type of the cached value
        });
    }

    private Uni<Object> readEntry(KeyValueEntry entry) {
        byte[] payload = entry.value().orElse(null);
        if (payload == null || payload.length == 0) {
            throw new CacheMiss(); // empty payload (tombstone or foreign data) → miss
        }
        ValueCodec.Envelope envelope = ValueCodec.decode(payload); // legacy/corrupt payload → CacheValueException (clean failure)
        if (envelope.isExpired(System.currentTimeMillis())) {
            throw new CacheMiss(); // logical TTL elapsed (design D6): re-run the loader; no server-side delete
        }
        return Uni.createFrom().item(envelope.data()); // null item = cached-null result → suppresses the loader
    }

    private <K, V> Uni<V> loadSync(K key, Function<K, V> valueLoader) {
        // Synchronous loaders are blocking: run them on the worker pool, never on a Vert.x event loop (design D3).
        return Uni.createFrom().item(() -> valueLoader.apply(key)).runSubscriptionOn(NatsCacheWorkers.EXECUTOR);
    }

    private static Long expirationOf(Duration expiresAfter) {
        if (expiresAfter == null) {
            return null;
        }
        if (expiresAfter.isNegative() || expiresAfter.isZero()) {
            throw new IllegalArgumentException("expiresAfter must be positive");
        }
        return System.currentTimeMillis() + expiresAfter.toMillis();
    }

    // ------------------------------------------------------------------ stampede protection (design D3)

    private <K, V> Uni<V> loadAndStore(K key, String encoded, Function<K, Uni<V>> loader, Long expiresAtEpochMillis) {
        return Uni.createFrom().deferred(() -> {
            CompletableFuture<Object> pending = new CompletableFuture<>();
            CompletableFuture<Object> existing = inFlight.putIfAbsent(encoded, pending);
            if (existing != null) {
                // Loser: await the winner's result (value or failure). The loader runs exactly once.
                return Uni.createFrom().completionStage(existing).map(o -> (V) o);
            }
            // Winner: load → store → settle the shared future for every waiter.
            return loader.apply(key)
                    .flatMap(value -> storeAndSettle(encoded, value, expiresAtEpochMillis, pending))
                    .onFailure().invoke(t -> {
                        if (!pending.isDone()) {
                            pending.completeExceptionally(t);
                        }
                    })
                    // Fires on success, failure and cancellation alike, so the map entry is always reclaimed.
                    .eventually(() -> inFlight.remove(encoded, pending));
        });
    }

    private <V> Uni<V> storeAndSettle(String encoded, V value, Long expiresAtEpochMillis, CompletableFuture<Object> pending) {
        return kv.put(encoded, ValueCodec.encode(value, expiresAtEpochMillis))
                .chain(entry -> {
                    // May be null (cached-null sentinel): complete(null) is legal and losers receive a null item.
                    pending.complete(value);
                    return Uni.createFrom().item(value);
                });
    }

    // ------------------------------------------------------------------ invalidation

    @Override
    public Uni<Void> invalidate(Object key) {
        return Uni.createFrom().deferred(() -> {
            Objects.requireNonNull(key, NULL_KEYS_NOT_SUPPORTED_MSG);
            String encoded = KeyCodec.encode(key);
            // purge (not delete): removes the key and all of its history; JNats purge is a no-op for absent keys.
            return ensureBucket().chain(() -> kv.purge(encoded)).replaceWithVoid();
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
                        .onItem().call(pair -> kv.purge(pair.encoded()))
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
        return Uni.createFrom().completionStage(readyFuture());
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

    /** Internal failure marker distinguishing "no usable entry" (miss / logically expired) from real errors. */
    private static final class CacheMiss extends RuntimeException {

        private CacheMiss() {
            super("cache miss");
        }
    }

    /** A stored key together with its decoded form, for {@link #invalidateIf(Predicate)}. */
    private record StoredKey(String encoded, Object decoded) {
    }
}
