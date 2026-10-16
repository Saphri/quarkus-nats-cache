package io.quarkiverse.nats.cache.runtime;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Shared worker pool for the (rare) blocking operations of the NATS KV cache backend: synchronous value
 * loaders on the cache side, and blocking NATS calls (the raw-JNats stream purge behind {@code invalidateAll()})
 * on the KV bucket side (design D3). These must never run on a Vert.x event loop.
 *
 * <p>Daemon threads are created on demand and reclaimed after 60 seconds of idleness, so an idle application
 * holds no threads; concurrent loads each get their own short-lived worker thread.
 */
public final class NatsCacheWorkers {

    public static final ExecutorService EXECUTOR = Executors.newCachedThreadPool(new ThreadFactory() {
        private final AtomicInteger sequence = new AtomicInteger();

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "quarkus-nats-cache-worker-" + sequence.incrementAndGet());
            t.setDaemon(true);
            return t;
        }
    });

    private NatsCacheWorkers() {
    }
}
