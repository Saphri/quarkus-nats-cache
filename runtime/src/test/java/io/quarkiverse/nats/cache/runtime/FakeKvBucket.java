package io.quarkiverse.nats.cache.runtime;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import io.quarkiverse.nats.cache.runtime.kv.CreateResult;
import io.quarkiverse.nats.cache.runtime.kv.KvBucket;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

/** In-memory {@link KvBucket} whose operations can be made to fail like an unreachable server. */
final class FakeKvBucket implements KvBucket {

    final Map<String, byte[]> entries = new ConcurrentHashMap<>();
    /** The per-item TTL of each successful create, in order ({@code null} = none). */
    final List<Duration> createTtls = new CopyOnWriteArrayList<>();
    final List<String> purges = new CopyOnWriteArrayList<>();
    final AtomicInteger provisions = new AtomicInteger();
    final AtomicInteger purgeAlls = new AtomicInteger();
    volatile boolean readsFail;
    volatile boolean writesFail;
    volatile boolean provisionFails;
    volatile boolean purgeAllFails;

    private static <T> Uni<T> connectionLost() {
        return Uni.createFrom().failure(new IllegalStateException("connection lost"));
    }

    @Override
    public Uni<Optional<byte[]>> get(String encodedKey) {
        return readsFail ? connectionLost() : Uni.createFrom().item(Optional.ofNullable(entries.get(encodedKey)));
    }

    @Override
    public Uni<CreateResult> create(String encodedKey, byte[] payload, Duration perItemTtlOrNull) {
        if (writesFail) {
            return connectionLost();
        }
        if (entries.putIfAbsent(encodedKey, payload) != null) {
            return Uni.createFrom().item(CreateResult.ALREADY_CACHED);
        }
        createTtls.add(perItemTtlOrNull);
        return Uni.createFrom().item(CreateResult.CREATED);
    }

    @Override
    public Uni<Void> purge(String encodedKey) {
        purges.add(encodedKey);
        entries.remove(encodedKey);
        return Uni.createFrom().voidItem();
    }

    @Override
    public Multi<String> keys() {
        return Multi.createFrom().iterable(new ArrayList<>(entries.keySet()));
    }

    @Override
    public Uni<Void> provision() {
        provisions.incrementAndGet();
        return provisionFails ? connectionLost() : Uni.createFrom().voidItem();
    }

    @Override
    public Uni<Void> purgeAll() {
        if (purgeAllFails) {
            return connectionLost();
        }
        purgeAlls.incrementAndGet();
        entries.clear();
        return Uni.createFrom().voidItem();
    }
}
