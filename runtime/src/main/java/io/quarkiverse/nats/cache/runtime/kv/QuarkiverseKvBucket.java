package io.quarkiverse.nats.cache.runtime.kv;

import static io.nats.client.support.NatsJetStreamConstants.JS_WRONG_LAST_SEQUENCE;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

import io.nats.client.JetStreamApiException;
import io.nats.client.support.NatsKeyValueUtil;
import io.quarkiverse.nats.cache.runtime.NatsCacheInfo;
import io.quarkiverse.nats.cache.runtime.NatsCacheWorkers;
import io.quarkiverse.reactive.messaging.nats.jetstream.client.Client;
import io.quarkiverse.reactive.messaging.nats.jetstream.client.store.KeyValue;
import io.quarkiverse.reactive.messaging.nats.jetstream.client.store.configuration.KeyValueConfiguration;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

/**
 * {@link KvBucket} over the quarkiverse nats-jetstream {@link Client} (and raw JNats for the whole-bucket purge).
 *
 * <p>Absorbs the transport quirks so the cache sees clean outcomes: a miss arrives as a {@code null} item and
 * becomes an empty optional; a create refused with {@code 10071} (wrong last sequence) becomes
 * {@link CreateResult#ALREADY_CACHED}; quarkiverse only offers create with a TTL, so an entry without one is written
 * with the longest TTL the client accepts; purge markers carry a short TTL, with a plain-purge fallback for buckets
 * that do not allow per-message TTLs.
 */
public class QuarkiverseKvBucket implements KvBucket {

    /** Longest per-message TTL the KV client accepts (whole seconds as an {@code int}). */
    private static final Duration MAX_TTL = Duration.ofSeconds(Integer.MAX_VALUE);

    /** Shortest per-message TTL the server accepts; the lifetime of every marker the extension leaves behind. */
    static final Duration SERVER_MINIMUM_TTL = Duration.ofSeconds(1);

    /** Lifetime of the marker a purge leaves behind. */
    static final Duration PURGE_MARKER_TTL = SERVER_MINIMUM_TTL;

    /**
     * How long the server keeps the delete marker it leaves when an entry expires (per-message TTL or bucket TTL).
     * Nothing in the extension reads markers ({@code get} treats them as a miss), so the server minimum is enough.
     */
    static final Duration LIMIT_MARKER_TTL = SERVER_MINIMUM_TTL;

    private static final byte[] EMPTY = new byte[0];

    private final Client client;
    private final NatsCacheInfo info;
    private final KeyValue kv;

    public QuarkiverseKvBucket(Client client, NatsCacheInfo info) {
        this.client = Objects.requireNonNull(client, "client must not be null");
        this.info = Objects.requireNonNull(info, "info must not be null");
        this.kv = client.keyValue(info.bucket());
    }

    @Override
    public Uni<Optional<byte[]>> get(String encodedKey) {
        // quarkiverse reports a miss as a null item; an entry without a value surfaces as an empty payload.
        return kv.get(encodedKey).map(entry -> entry == null ? Optional.empty()
                : Optional.of(entry.value().orElse(EMPTY)));
    }

    @Override
    public Uni<CreateResult> create(String encodedKey, byte[] payload, Duration perItemTtlOrNull) {
        // The quarkiverse KeyValue only offers create with a TTL: an entry without one gets the longest TTL the
        // client accepts, and the bucket's own max age (if any) still applies.
        return kv.create(encodedKey, payload, perItemTtlOrNull != null ? perItemTtlOrNull : MAX_TTL)
                .map(ignored -> CreateResult.CREATED)
                .onFailure(QuarkiverseKvBucket::isWrongLastSequence).recoverWithItem(CreateResult.ALREADY_CACHED);
    }

    private static boolean isWrongLastSequence(Throwable failure) {
        // quarkiverse wraps the JNats failure; walk the cause chain.
        for (Throwable t = failure; t != null; t = t.getCause()) {
            if (t instanceof JetStreamApiException e) {
                return e.getApiErrorCode() == JS_WRONG_LAST_SEQUENCE;
            }
        }
        return false;
    }

    /**
     * The purge marker carries a short TTL so the server removes it too: without one every invalidated key would
     * leave a marker behind for good. A bucket that does not allow per-message TTLs (created externally) refuses
     * that, so fall back to a plain purge.
     */
    @Override
    public Uni<Void> purge(String encodedKey) {
        return kv.purge(encodedKey, PURGE_MARKER_TTL).onFailure().recoverWithUni(() -> kv.purge(encodedKey));
    }

    @Override
    public Multi<String> keys() {
        return kv.keys();
    }

    @Override
    public Uni<Void> provision() {
        return client.keyValueManagement().addIfAbsent(keyValueConfiguration(info));
    }

    @Override
    public Uni<Void> purgeAll() {
        // JNats has no purgeAll: purge the whole KV stream. Blocking raw-JNats call → worker pool.
        return Uni.createFrom().<Void> emitter(emitter -> {
            try {
                client.nativeConnection().jetStreamManagement()
                        .purgeStream(NatsKeyValueUtil.toStreamName(info.bucket()));
                emitter.complete(null);
            } catch (Exception e) {
                emitter.fail(e);
            }
        }).runSubscriptionOn(NatsCacheWorkers.EXECUTOR);
    }

    /**
     * The configuration used to provision the bucket (add-if-absent). Setting a limit marker is how a KV bucket
     * enables per-message TTLs ({@code allow_msg_ttl}), which carry per-item expiration; this requires NATS server
     * 2.11 or later.
     */
    static KeyValueConfiguration keyValueConfiguration(NatsCacheInfo info) {
        io.nats.client.api.KeyValueConfiguration.Builder builder = io.nats.client.api.KeyValueConfiguration
                .builder(info.bucket());
        info.ttl().ifPresent(builder::ttl);
        builder.maxHistoryPerKey(info.history());
        builder.limitMarker(LIMIT_MARKER_TTL);
        return KeyValueConfiguration.of(builder.build());
    }
}
