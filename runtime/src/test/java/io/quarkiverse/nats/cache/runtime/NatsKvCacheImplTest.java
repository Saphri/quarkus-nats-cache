package io.quarkiverse.nats.cache.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import io.quarkiverse.nats.cache.runtime.codec.CacheTypeAllowList;
import io.quarkiverse.nats.cache.runtime.codec.KeyCodec;
import io.quarkiverse.nats.cache.runtime.codec.ValueCodec;
import io.quarkus.cache.CacheException;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.smallrye.mutiny.subscription.Cancellable;
import io.smallrye.mutiny.subscription.UniEmitter;

/**
 * Behaviour of the cache policy against an in-memory {@link FakeKvBucket}: fail-open reads and writes, create-only
 * writes, per-item expiration, invalidation, provisioning retry and stampede protection. No NATS server needed.
 */
class NatsKvCacheImplTest {

    private final FakeKvBucket fake = new FakeKvBucket();
    private final NatsKvCacheImpl cache = new NatsKvCacheImpl(
            NatsCacheInfo.of("test", Optional.empty(), Optional.empty(), 1), fake,
            new KeyCodec(CacheTypeAllowList.jdkOnly()), new ValueCodec(CacheTypeAllowList.jdkOnly()));
    private final KeyCodec keyCodec = new KeyCodec(CacheTypeAllowList.jdkOnly());

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

        // No expiresAfter → no per-item TTL at the seam (the adapter decides how to write that).
        assertThat(fake.createTtls).containsExactly(Duration.ofMinutes(5), null);
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
    void expiresAfterBeyondTheClientLimitIsCapped() {
        assertThat(NatsKvCacheImpl.messageTtlOf(Duration.ofDays(30))).isEqualTo(Duration.ofDays(30));
        assertThat(NatsKvCacheImpl.messageTtlOf(Duration.ofSeconds(Long.MAX_VALUE)))
                .isEqualTo(Duration.ofSeconds(Integer.MAX_VALUE));
        assertThat(NatsKvCacheImpl.messageTtlOf(null)).isNull();
    }

    @Test
    void invalidatePurgesTheKey() {
        await(cache.<String, String> get("k", k -> "v"));
        await(cache.invalidate("k"));

        assertThat(fake.entries).isEmpty();
        assertThat(fake.purges).containsExactly(keyCodec.encode("k"));
    }

    @Test
    void invalidateAllPurgesTheWholeBucket() {
        await(cache.<String, String> get("a", k -> "1"));
        await(cache.<String, String> get("b", k -> "2"));

        await(cache.invalidateAll());

        assertThat(fake.entries).isEmpty();
        assertThat(fake.purgeAlls).hasValue(1);
    }

    @Test
    void invalidateAllFailsWhenProvisioningFails() {
        fake.provisionFails = true;

        assertThatThrownBy(() -> await(cache.invalidateAll())).hasMessageContaining("connection lost");
        assertThat(fake.purgeAlls).hasValue(0); // never purges a bucket that may not exist
    }

    @Test
    void invalidateAllFailsWhenThePurgeFails() {
        fake.purgeAllFails = true;

        assertThatThrownBy(() -> await(cache.invalidateAll())).isInstanceOf(CacheException.class);
    }

    @Test
    void invalidateIfPurgesTheMatchingKeys() {
        await(cache.<String, String> get("keep", k -> "1"));
        await(cache.<String, String> get("drop-1", k -> "2"));
        await(cache.<String, String> get("drop-2", k -> "3"));

        await(cache.invalidateIf(k -> ((String) k).startsWith("drop")));

        assertThat(fake.purges).containsExactlyInAnyOrder(keyCodec.encode("drop-1"), keyCodec.encode("drop-2"));
        assertThat(fake.entries).containsOnlyKeys(keyCodec.encode("keep"));
    }

    @Test
    void invalidateIfSkipsStoredKeysThatCannotBeDecoded() {
        await(cache.<String, String> get("drop", k -> "1"));
        fake.entries.put("not a codec key", "{\"data\":\"foreign\"}".getBytes(StandardCharsets.UTF_8));

        await(cache.invalidateIf(k -> true));

        assertThat(fake.purges).containsExactly(keyCodec.encode("drop"));
        assertThat(fake.entries).containsOnlyKeys("not a codec key");
    }

    @Test
    void provisioningIsRetriedOnTheNextOperationAfterAFailure() {
        fake.provisionFails = true;
        // Fails open: the loaded value is still returned.
        assertThat(await(cache.<String, String> get("k", k -> "loaded"))).isEqualTo("loaded");
        assertThat(fake.provisions).hasValue(1);

        fake.provisionFails = false;
        await(cache.invalidate("k"));
        assertThat(fake.provisions).hasValue(2);

        await(cache.invalidate("k"));
        assertThat(fake.provisions).hasValue(2); // a successful provisioning is memoized
    }

    @Test
    void aFailingLoaderPropagatesToStampedeWaiters() {
        UniEmitter<? super String>[] emitter = new UniEmitter[1];
        AtomicInteger loads = new AtomicInteger();
        java.util.function.Function<String, Uni<String>> loader = k -> {
            loads.incrementAndGet();
            return Uni.createFrom().emitter(e -> emitter[0] = e);
        };

        UniAssertSubscriber<String> winner = cache.getAsync("k", loader).subscribe()
                .withSubscriber(UniAssertSubscriber.create());
        UniAssertSubscriber<String> waiter = cache.getAsync("k", loader).subscribe()
                .withSubscriber(UniAssertSubscriber.create());

        emitter[0].fail(new IllegalStateException("loader failed"));

        winner.awaitFailure(Duration.ofSeconds(5)).assertFailedWith(IllegalStateException.class, "loader failed");
        waiter.awaitFailure(Duration.ofSeconds(5)).assertFailedWith(IllegalStateException.class, "loader failed");
        assertThat(loads).hasValue(1);
        assertThat(fake.entries).isEmpty(); // nothing cached
    }

    @Test
    void nonPositiveExpiresAfterIsRejected() {
        assertThatThrownBy(() -> cache.get("k", k -> "v", Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> cache.get("k", k -> "v", Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
