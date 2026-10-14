package io.quarkiverse.nats.cache.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import io.nats.client.JetStreamApiException;
import io.nats.client.MessageTtl;
import io.quarkiverse.reactive.messaging.nats.jetstream.client.Client;
import io.quarkiverse.reactive.messaging.nats.jetstream.client.connection.NativeConnection;
import io.quarkiverse.reactive.messaging.nats.jetstream.client.store.KeyValue;
import io.quarkiverse.reactive.messaging.nats.jetstream.client.store.KeyValueManagement;
import io.quarkiverse.reactive.messaging.nats.jetstream.client.store.api.KeyValueEntry;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.smallrye.mutiny.subscription.Cancellable;
import io.smallrye.mutiny.subscription.UniEmitter;

/**
 * Behaviour of the cache implementation against an in-memory fake of the KV clients: fail-open reads and writes,
 * create-only writes, per-item expiration as a per-message TTL, and stampede protection under cancellation. No NATS
 * server needed.
 */
class NatsKvCacheImplTest {

    /** Map-backed KV whose reads and writes can be made to fail like an unreachable server. */
    static final class FakeKv {
        final Map<String, byte[]> entries = new ConcurrentHashMap<>();
        /** The per-message TTL of each successful create, in order ({@code null} = no TTL). */
        final List<MessageTtl> createTtls = new CopyOnWriteArrayList<>();
        final List<String> purges = new CopyOnWriteArrayList<>();
        volatile boolean readsFail;
        volatile boolean writesFail;

        KeyValue keyValue() {
            return (KeyValue) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] { KeyValue.class },
                    (proxy, method, args) -> switch (method.getName()) {
                        case "get" -> readsFail ? Uni.createFrom().failure(new IllegalStateException("connection lost"))
                                : Uni.createFrom().item(entry((String) args[0], entries.get((String) args[0])));
                        case "purge" -> {
                            purges.add((String) args[0]);
                            entries.remove((String) args[0]);
                            yield Uni.createFrom().voidItem();
                        }
                        case "bucketName" -> "TEST";
                        default -> throw new UnsupportedOperationException(method.getName());
                    });
        }

        Client client() {
            KeyValue kv = keyValue();
            KeyValueManagement management = (KeyValueManagement) Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class<?>[] { KeyValueManagement.class }, (proxy, method, args) -> {
                        if (method.getName().equals("addIfAbsent")) {
                            return Uni.createFrom().voidItem();
                        }
                        throw new UnsupportedOperationException(method.getName());
                    });
            // JNats KeyValue: writes are create-only, so an existing key is refused like the server does.
            io.nats.client.KeyValue nativeKv = (io.nats.client.KeyValue) Proxy.newProxyInstance(
                    getClass().getClassLoader(), new Class<?>[] { io.nats.client.KeyValue.class },
                    (proxy, method, args) -> {
                        if (!method.getName().equals("create") || args.length != 3) {
                            throw new UnsupportedOperationException(method.getName());
                        }
                        if (writesFail) {
                            throw new IllegalStateException("connection lost");
                        }
                        if (entries.putIfAbsent((String) args[0], (byte[]) args[1]) != null) {
                            throw new WrongLastSequence();
                        }
                        createTtls.add((MessageTtl) args[2]);
                        return (long) createTtls.size();
                    });
            NativeConnection connection = (NativeConnection) Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class<?>[] { NativeConnection.class }, (proxy, method, args) -> switch (method.getName()) {
                        case "keyValue" -> nativeKv;
                        default -> throw new UnsupportedOperationException(method.getName());
                    });
            return (Client) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] { Client.class },
                    (proxy, method, args) -> switch (method.getName()) {
                        case "keyValue" -> kv;
                        case "keyValueManagement" -> management;
                        case "nativeConnection" -> connection;
                        default -> throw new UnsupportedOperationException(method.getName());
                    });
        }

        private static KeyValueEntry entry(String key, byte[] value) {
            if (value == null) {
                return null; // quarkiverse 3.39.x reports a miss as a null item
            }
            return (KeyValueEntry) Proxy.newProxyInstance(FakeKv.class.getClassLoader(),
                    new Class<?>[] { KeyValueEntry.class }, (proxy, method, args) -> switch (method.getName()) {
                        case "key" -> key;
                        case "value" -> Optional.of(value);
                        default -> throw new UnsupportedOperationException(method.getName());
                    });
        }
    }

    /** What the server answers to a create on an existing key (JNats has no public constructor for its Error). */
    static final class WrongLastSequence extends JetStreamApiException {
        WrongLastSequence() {
            super(io.nats.client.api.Error.JsBadRequestErr);
        }

        @Override
        public int getApiErrorCode() {
            return 10071;
        }
    }

    private final FakeKv fake = new FakeKv();
    private final NatsKvCacheImpl cache = new NatsKvCacheImpl(
            NatsCacheInfo.of("test", Optional.empty(), Optional.empty(), 1), fake.client());

    private static <T> T await(Uni<T> uni) {
        return uni.await().atMost(Duration.ofSeconds(5));
    }

    @Test
    void unreachableServerFallsThroughToTheLoader() {
        fake.readsFail = true;
        fake.writesFail = true;
        assertThat(await(cache.<String, String> get("k", k -> "loaded"))).isEqualTo("loaded");
    }

    @Test
    void failedWriteStillReturnsTheLoadedValue() {
        fake.writesFail = true;
        assertThat(await(cache.<String, String> get("k", k -> "loaded"))).isEqualTo("loaded");
        assertThat(fake.entries).isEmpty();
    }

    @Test
    void undecodableEntryIsPurgedAndReplaced() {
        assertThat(await(cache.<String, String> get("k", k -> "first"))).isEqualTo("first");
        String stored = fake.entries.keySet().iterator().next();
        fake.entries.put(stored, "{\"data\":{\"@class\":\"com.example.Gadget\"}}".getBytes(StandardCharsets.UTF_8));

        assertThat(await(cache.<String, String> get("k", k -> "reloaded"))).isEqualTo("reloaded");
        assertThat(fake.purges).containsExactly(stored); // dropped first: entries are never overwritten in place
        assertThat(await(cache.<String, String> get("k", k -> "not called"))).isEqualTo("reloaded");
    }

    @Test
    void cancellingTheLoadingCallerDoesNotStrandWaiters() {
        AtomicInteger loads = new AtomicInteger();
        AtomicInteger cancellations = new AtomicInteger();
        UniEmitter<? super String>[] emitter = new UniEmitter[1];
        java.util.function.Function<String, Uni<String>> loader = k -> {
            loads.incrementAndGet();
            return Uni.createFrom().<String> emitter(e -> emitter[0] = e)
                    .onCancellation().invoke(cancellations::incrementAndGet);
        };

        // The first caller wins the load, a second one waits on it; then the first caller goes away.
        UniAssertSubscriber<String> winner = cache.getAsync("k", loader).subscribe()
                .withSubscriber(UniAssertSubscriber.create());
        UniAssertSubscriber<String> waiter = cache.getAsync("k", loader).subscribe()
                .withSubscriber(UniAssertSubscriber.create());
        winner.cancel();

        emitter[0].complete("value");

        waiter.awaitItem(Duration.ofSeconds(5)).assertItem("value");
        assertThat(loads).hasValue(1);
        assertThat(cancellations).hasValue(0); // the shared load is not tied to any one caller
        assertThat(await(cache.<String, String> get("k", k -> "not called"))).isEqualTo("value");
    }

    @Test
    void cancellingAWaiterDoesNotAffectOtherCallers() {
        UniEmitter<? super String>[] emitter = new UniEmitter[1];
        java.util.function.Function<String, Uni<String>> loader = k -> Uni.createFrom().emitter(e -> emitter[0] = e);

        UniAssertSubscriber<String> winner = cache.getAsync("k", loader).subscribe()
                .withSubscriber(UniAssertSubscriber.create());
        Cancellable waiter = cache.getAsync("k", loader).subscribe().with(v -> {
        });
        waiter.cancel();

        emitter[0].complete("value");
        winner.awaitItem(Duration.ofSeconds(5)).assertItem("value");
    }

    @Test
    void expiresAfterIsWrittenAsAPerMessageTtl() {
        assertThat(await(cache.<String, String> get("k", k -> "loaded", Duration.ofMinutes(5)))).isEqualTo("loaded");
        assertThat(await(cache.<String, String> get("plain", k -> "loaded"))).isEqualTo("loaded");

        assertThat(fake.createTtls).hasSize(2);
        assertThat(fake.createTtls.get(0).getTtlString()).isEqualTo("300s");
        assertThat(fake.createTtls.get(1)).isNull(); // no expiresAfter → no per-message TTL
        assertThat(fake.entries.values()).allSatisfy(
                v -> assertThat(new String(v, StandardCharsets.UTF_8)).isEqualTo("{\"data\":\"loaded\"}"));
    }

    @Test
    void anEntryCachedByAnotherWriterIsNotOverwritten() {
        // Another instance cached the key between this instance's miss and its write: first write wins, and the
        // loaded value is still returned.
        assertThat(await(cache.<String, String> get("k", k -> "first"))).isEqualTo("first");
        byte[] stored = fake.entries.values().iterator().next();
        fake.readsFail = true; // force this instance down the miss → load → create path

        assertThat(await(cache.<String, String> get("k", k -> "second"))).isEqualTo("second");
        assertThat(fake.entries.values()).containsExactly(stored);
    }

    @Test
    void expiresAfterIsRoundedUpToWholeSeconds() {
        assertThat(NatsKvCacheImpl.messageTtlOf(Duration.ofSeconds(2)).getTtlString()).isEqualTo("2s");
        assertThat(NatsKvCacheImpl.messageTtlOf(Duration.ofMillis(1)).getTtlString()).isEqualTo("1s");
        assertThat(NatsKvCacheImpl.messageTtlOf(Duration.ofMillis(1500)).getTtlString()).isEqualTo("2s");
        assertThat(NatsKvCacheImpl.messageTtlOf(Duration.ofDays(30)).getTtlString()).isEqualTo("2592000s");
        assertThat(NatsKvCacheImpl.messageTtlOf(null)).isNull();
        assertThat(NatsKvCacheImpl.messageTtlOf(Duration.ofSeconds(Long.MAX_VALUE)).getTtlString())
                .isEqualTo(Integer.MAX_VALUE + "s");
    }

    @Test
    void nonPositiveExpiresAfterIsRejected() {
        assertThatThrownBy(() -> cache.get("k", k -> "v", Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> cache.get("k", k -> "v", Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
