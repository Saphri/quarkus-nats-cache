package io.quarkiverse.nats.cache.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import io.nats.client.JetStream;
import io.nats.client.KeyValueManagement;
import io.nats.client.Message;
import io.nats.client.PublishOptions;
import io.quarkiverse.reactive.messaging.nats.jetstream.client.Client;
import io.quarkiverse.reactive.messaging.nats.jetstream.client.connection.NativeConnection;
import io.quarkiverse.reactive.messaging.nats.jetstream.client.store.KeyValue;
import io.quarkiverse.reactive.messaging.nats.jetstream.client.store.api.KeyValueEntry;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.smallrye.mutiny.subscription.Cancellable;
import io.smallrye.mutiny.subscription.UniEmitter;

/**
 * Behaviour of the cache implementation against an in-memory fake of the quarkiverse KV client: fail-open
 * reads and writes, per-item expiration as a per-message TTL, and stampede protection under cancellation. No NATS
 * server needed.
 */
class NatsKvCacheImplTest {

    /** Map-backed KV whose reads and writes can be made to fail like an unreachable server. */
    static final class FakeKv {
        final Map<String, byte[]> entries = new ConcurrentHashMap<>();
        /** TTL-carrying puts, which bypass the quarkiverse KV and are published to the key subject directly. */
        final List<Message> ttlPublishes = new CopyOnWriteArrayList<>();
        final List<PublishOptions> ttlPublishOptions = new CopyOnWriteArrayList<>();
        volatile boolean readsFail;
        volatile boolean writesFail;

        KeyValue keyValue() {
            return (KeyValue) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] { KeyValue.class },
                    (proxy, method, args) -> switch (method.getName()) {
                        case "get" -> readsFail ? Uni.createFrom().failure(new IllegalStateException("connection lost"))
                                : Uni.createFrom().item(entry((String) args[0], entries.get((String) args[0])));
                        case "put" -> {
                            if (writesFail) {
                                yield Uni.createFrom().failure(new IllegalStateException("connection lost"));
                            }
                            entries.put((String) args[0], (byte[]) args[1]);
                            yield Uni.createFrom().item(entry((String) args[0], (byte[]) args[1]));
                        }
                        case "bucketName" -> "TEST";
                        default -> throw new UnsupportedOperationException(method.getName());
                    });
        }

        Client client() {
            KeyValue kv = keyValue();
            KeyValueManagement management = (KeyValueManagement) Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class<?>[] { KeyValueManagement.class }, (proxy, method, args) -> {
                        if (method.getName().equals("getBucketNames")) {
                            return List.of("TEST"); // already provisioned
                        }
                        throw new UnsupportedOperationException(method.getName());
                    });
            JetStream jetStream = (JetStream) Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class<?>[] { JetStream.class }, (proxy, method, args) -> {
                        if (method.getName().equals("publishAsync") && args.length == 2) {
                            ttlPublishes.add((Message) args[0]);
                            ttlPublishOptions.add((PublishOptions) args[1]);
                            return CompletableFuture.completedFuture(null);
                        }
                        throw new UnsupportedOperationException(method.getName());
                    });
            NativeConnection connection = (NativeConnection) Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class<?>[] { NativeConnection.class }, (proxy, method, args) -> switch (method.getName()) {
                        case "keyValueManagement" -> management;
                        case "jetStream" -> jetStream;
                        default -> throw new UnsupportedOperationException(method.getName());
                    });
            return (Client) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] { Client.class },
                    (proxy, method, args) -> switch (method.getName()) {
                        case "keyValue" -> kv;
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

    private final FakeKv fake = new FakeKv();
    private final NatsKvCacheImpl cache = new NatsKvCacheImpl(
            NatsCacheInfo.of("test", Optional.empty(), Optional.empty(), 1), fake.client());

    private static <T> T await(Uni<T> uni) {
        return uni.await().atMost(Duration.ofSeconds(5));
    }

    /**
     * Completes the (asynchronous) bucket provisioning up front, so that a subsequent {@code getAsync} reaches its
     * loader synchronously on subscription and the test can drive the loader's emitter deterministically.
     */
    private void provisionBucket() {
        await(cache.<String, String> get("warm-up", k -> "w"));
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
    void undecodableEntryIsReloadedAndOverwritten() {
        assertThat(await(cache.<String, String> get("k", k -> "first"))).isEqualTo("first");
        String stored = fake.entries.keySet().iterator().next();
        fake.entries.put(stored, "{\"data\":{\"@class\":\"com.example.Gadget\"}}".getBytes(StandardCharsets.UTF_8));

        assertThat(await(cache.<String, String> get("k", k -> "reloaded"))).isEqualTo("reloaded");
        assertThat(await(cache.<String, String> get("k", k -> "not called"))).isEqualTo("reloaded");
    }

    @Test
    void cancellingTheLoadingCallerDoesNotStrandWaiters() {
        provisionBucket();
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
        provisionBucket();
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
    void expiresAfterIsPublishedAsAPerMessageTtl() {
        assertThat(await(cache.<String, String> get("k", k -> "loaded", Duration.ofMinutes(5)))).isEqualTo("loaded");

        assertThat(fake.entries).isEmpty(); // not written through the TTL-less quarkiverse put
        assertThat(fake.ttlPublishes).singleElement().satisfies(m -> {
            assertThat(m.getSubject()).startsWith("$KV.TEST.");
            assertThat(new String(m.getData(), StandardCharsets.UTF_8)).isEqualTo("{\"data\":\"loaded\"}");
        });
        assertThat(fake.ttlPublishOptions).singleElement()
                .satisfies(o -> assertThat(o.getMessageTtl()).isEqualTo("300s"));
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
